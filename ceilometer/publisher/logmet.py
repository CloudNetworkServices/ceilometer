#
# Copyright 2016 IBM Corp
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_log import log
from six.moves.urllib import parse as urlparse
import time
import ceilometer
from ceilometer import publisher
from mtgraphite import MTGraphiteClient
import datetime
import requests
import json


from Queue import Queue 
import threading

DEFAULT_QUEUE_LEN = 1024

LOG = log.getLogger(__name__)


class mtg_sender(threading.Thread):  
  
    def __init__(self, t_name, queue, mtgclient,
                 ccsapi_protocl, ccsapi_host, ccsapi_port, ccsapi_user, ccsapi_password):  
        threading.Thread.__init__(self, name=t_name)  
        self.queue = queue  
        self.mtgclient = mtgclient
        self.ccsapi_protocl = ccsapi_protocl
        self.ccsapi_host = ccsapi_host
        self.ccsapi_port = ccsapi_port
        self.ccsapi_user = ccsapi_user
        self.ccsapi_password = ccsapi_password
        self.space_id_cache = {}
        now_stamp = time.time()
        local_time = datetime.datetime.fromtimestamp(now_stamp)
        utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
        self.time_offset = local_time - utc_time

    def get_space_id(self, tenant_id):
        LOG.debug(("Now get space id by %s" % tenant_id))
        if tenant_id in self.space_id_cache:
            LOG.debug(("Got space id in cache %s" % self.space_id_cache[tenant_id]))
            return self.space_id_cache[tenant_id]

        host = self.ccsapi_protocl + ':' + self.ccsapi_host + ':' + self.ccsapi_port + '/v3/admin/spaceid/' + tenant_id
        LOG.debug(("host %s" % host))
        LOG.debug(("user %s" % self.ccsapi_user))
        LOG.debug(("password %s" % self.ccsapi_password))
        reponse = requests.get(host, auth=(self.ccsapi_user, self.ccsapi_password))
        if reponse.status_code != 200:
            LOG.warn(("Can not get space id"))
            self.space_id_cache[tenant_id] = None
            return None
        else:
            space_id = json.loads(reponse.text)['space_org_id']['space_uuid']
            self.space_id_cache[tenant_id] = space_id
            LOG.debug(("space id is %s" % space_id))
            return space_id

    def construct_bandwidth_msg(self, sample):
        metadata = sample.resource_metadata

        if not metadata.has_key("label_name"):
            return None, None

        label_name = metadata["label_name"]

        if label_name[-len("_rx_esp"):] != "_rx_esp" and label_name[-len("_tx_esp"):] != "_tx_esp" :
            return None, None

        conn_name = label_name[:-len('rx_esp')-1]

        conn_type = label_name[-len("rx_esp"):-4]
        octets = sample.volume
        if not metadata.has_key("pkts"):
            return None, None
        pkts = metadata["pkts"]

        if metadata.has_key("last_update"):
            time_sec = metadata["last_update"]
        else:
            time_sec = str(int(time.time()))

        if sample.project_id == None:
            return None, None

        space_id = self.get_space_id(sample.project_id)

        if space_id == None:
            return None, None

        if self.mtgclient.is_super:
            msg_octets = '%s.%s.%s.%s.%s.%s %d %s\r\n' %(space_id, "0000", "vpngateway0001",conn_name,"vpn_octets",conn_type, octets, time_sec)
            msg_packets = '%s.%s.%s.%s.%s.%s %d %s\r\n' %(space_id, "0000", "vpngateway0001",conn_name,"vpn_packets",conn_type, pkts, time_sec)
        else:
            msg_octets = '%s.%s.%s.%s.%s.%s %d %s\r\n' %(self.tenant_id, "0000", "vpngateway0001",conn_name,"vpn_octets",conn_type, octets, time_sec)
            msg_packets = '%s.%s.%s.%s.%s.%s %d %s\r\n' %(self.tenant_id, "0000", "vpngateway0001",conn_name,"vpn_packets",conn_type, pkts, time_sec)
 
        return msg_octets, msg_packets

    def construct_status_msg(self, sample):
        metadata = sample.resource_metadata

        if not metadata.has_key("status"):
            return None

        if metadata["status"] == "ACTIVE" :
            status = 1
        else:
            status = -1

        conn_name = metadata["name"]

        try:
            timestamp = sample.timestamp
            utc_time = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
            local_st = utc_time+self.time_offset
            time_sec = int(time.mktime(local_st.timetuple()))
        except Exception as e:
            #time_sec = str(int(time.time()))
            pass

        if sample.project_id == None:
            return None

        space_id = self.get_space_id(sample.project_id)

        if space_id == None:
            return None

        if self.mtgclient.is_super:
            msg_status = '%s.%s.%s.%s.%s %s %s\r\n' %(space_id, "0000", "vpngateway0001",conn_name,"vpn_status", status, time_sec)
        else:
            msg_status = '%s.%s.%s.%s.%s %s %s\r\n' %(self.tenant_id, "0000", "vpngateway0001",conn_name,"vpn_status", status, time_sec)
            
        return msg_status

    def construct_msg(self, samples):
        msgset = []
        for sample in samples:     
            if sample.name == "bandwidth":
                msg_octets, msg_packets = self.construct_bandwidth_msg(sample)
                if msg_octets == None:
                    continue
                msgset.append(str(msg_octets))
                msgset.append(str(msg_packets))

            elif sample.name == "network.services.vpn.connections":
                msg_status = self.construct_status_msg(sample)
                if msg_status == None:
                    continue
                msgset.append(str(msg_status))
            else:
                continue

        return msgset
  
    def run(self):  
        while True:  
            try:
                LOG.debug(("start getting message from queue"))
                samples = self.queue.get()  
                msgset = self.construct_msg(samples)

                LOG.debug(("get a msgset %s" % msgset))

                if len(msgset):
                    num_pushed_to_queue = self.mtgclient.send_messages(msgset)
                    LOG.debug(("Pushed %d messages to mtgraphite queue" % num_pushed_to_queue))
            except Exception as e:
                LOG.debug(("Pushed samples to mtgraphite failed %s" % e))
         

class LotmetPublisher(publisher.PublisherBase):
    """Publisher metering data to file.

    The publisher which records metering data into a file. The file name and
    location should be configured in ceilometer pipeline configuration file.
    If a file name and location is not specified, this File Publisher will not
    log any meters other than log a warning in Ceilometer log file.

    To enable this publisher, add the following section to the
    /etc/ceilometer/publisher.yaml file or simply add it to an existing
    pipeline::

        -
            name: meter_file
            interval: 600
            counters:
                - "*"
            transformers:
            publishers:
                - logmet://mtgraphite://metrics.stage1.opvis.bluemix.net:9095/super:vpnService:password:http://9.197.41.60:8081:alchemy:password

    File path is required for this publisher to work properly. If max_bytes
    or backup_count is missing, FileHandler will be used to save the metering
    data. If max_bytes and backup_count are present, RotatingFileHandler will
    be used to save the metering data.
    """

    def __init__(self, url):
        """ init for the logmet publisher, url,space.and, path
            Please define url as this: mtgraphite://8.8.8.8:9092/super:tenant_id:password:http://9.197.41.60:8081:alchemy:docker4ever
        """

        list = url.path[len('//'):].split('/', 1)
        self.host = list[0].split(':')[0]
        self.port = list[0].split(':')[1]

        if list[1].split(':')[0] == 'super':
            self.is_super = True
        else:
            self.is_super = False

        self.tenant_id = list[1].split(':', 8)[1]
        self.tenant_password = list[1].split(':', 8)[2]
        self.ccsapi_protocl = list[1].split(':', 8)[3]
        self.ccsapi_host = list[1].split(':', 8)[4]
        self.ccsapi_port = list[1].split(':', 8)[5]
        self.ccsapi_user = list[1].split(':', 8)[6]
        self.ccsapi_password = list[1].split(':', 8)[7]

        LOG.debug(("%s, %s, %s, %s, %s, %s" % (self.is_super, self.port, self.tenant_password, self.host, self.tenant_id, self.tenant_password)))

        self.queue = Queue(maxsize = DEFAULT_QUEUE_LEN)  
        
        self.mtgclient = MTGraphiteClient(self.host, self.port, self.is_super, self.tenant_id, self.tenant_password)
        self.msg_sender_thread = mtg_sender("vpn_mgt_sender", self.queue, self.mtgclient, 
                                            self.ccsapi_protocl,self.ccsapi_host, self.ccsapi_port, self.ccsapi_user,self.ccsapi_password)
        self.msg_sender_thread.setDaemon(True)
        self.msg_sender_thread.start()
    

    def publish_samples(self, context, samples):
        """Send a metering message for publishing

        :param context: Execution context from the service or RPC call
        :param samples: Samples from pipeline after transformation
        """
        
        if self.queue.full() :
            LOG.debug((" The queue is full and the sample %s will be dropt" % samples))
            return 
            
        self.queue.put(samples)
        LOG.debug(("pushed a msg set %s" % samples))

    def publish_events(self, context, events):
        """Send an event message for publishing

        :param context: Execution context from the service or RPC call
        :param events: events from pipeline after transformation
        """
        raise ceilometer.NotImplementedError



