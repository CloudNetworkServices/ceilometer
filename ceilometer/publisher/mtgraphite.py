#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import socket
import ssl
import struct
import time
from oslo_log import log
# This code is based upon the Kafka producer/client classes
LOG = log.getLogger(__name__)

DEFAULT_SOCKET_TIMEOUT_SECONDS = 600
class MTGraphiteClient(object):

    def __init__(self, host, port, is_super, tenant_id, tenant_password, batch_send_every_t = 5,
                 batch_send_every_n = 50):
        self.host = host
        self.port = port
        self.is_super = is_super
        self.tenant_id = tenant_id
        self.tenant_password = tenant_password

        # create a connection only when we need it, but keep it alive
        self.conn = None
        self.socket = None
        self.batch_send_every_n = batch_send_every_n
        self.batch_send_every_t = batch_send_every_t
        self.msgset = []
        self.next_timeout = time.time() + batch_send_every_t

    ##################
    #   Private API  #
    ##################

    def _get_socket(self):
        '''Get or create a connection to a broker using host and port'''

        if self.conn is not None:
            return self.conn

        LOG.debug('Creating a new socket with _get_socket()')
        while self.conn is None:
            try:
                self.sequence = 1  # start with 1 as last_ack = 0
                self.socket = socket.socket(socket.AF_INET,
                                            socket.SOCK_STREAM)
                self.socket.settimeout(DEFAULT_SOCKET_TIMEOUT_SECONDS)
                self.conn = ssl.wrap_socket(self.socket,
                                            cert_reqs=ssl.CERT_NONE)
                self.conn.connect((self.host, int(self.port)))


                # We send this identifier message so that the server-side can
                # identify this specific crawler in the logs (its behind
                # load-balancer so it never sees our source-ip without this).

                self_identifier = str(self.conn.getsockname()[0])
                LOG.debug('self_identifier = %s', self_identifier)
                identification_message = """"""
                identification_message += '1I'
                identification_message += chr(len(self_identifier))
                identification_message += self_identifier
                sent = self.conn.write(identification_message)
                if sent != len(identification_message):
                    LOG.warning("Identification message not sent properly, returned len = %d", sent)
                authentication_message = """"""
                if self.is_super:
                    authentication_message += "2S"
                else :
                    authentication_message = """"""
                    authentication_message += "2T"

                authentication_message += chr(len(self.tenant_id))
                authentication_message += self.tenant_id
                authentication_message += chr(len(self.tenant_password))
                authentication_message += self.tenant_password
                LOG.info(
                    'Sent authentication mesg %s with mtgraphite' % authentication_message)
                sent = self.conn.write(authentication_message)
                LOG.info(
                    'Sent authentication with mtgraphite, returned length = '
                    '%d' % sent)
                if sent != len(authentication_message):
                    raise Exception('failed to send tenant/password')
                chunk = self.conn.read(6)  # Expecting "1A"
                code = bytearray(chunk)[:2]
                LOG.info('MTGraphite authentication server response of %s'
                            % code)
                if code == '0A':
                    raise Exception('Invalid tenant auth, please check the '
                                    'tenant id or password!')
                return self.conn
            except Exception as e:
                LOG.exception(e)
                if self.conn is not None:
                    self.conn.close()
                    self.conn = None
                time.sleep(2)  # sleep for 2 seconds for now

    def _write_messages_no_retries(self, msgset):
        s = self._get_socket()
        messages_string = bytearray('1W')
        messages_string.extend(bytearray(struct.pack('!I',
                                                     len(msgset))))
        for m in msgset:
            if m == msgset[0]:

                # LOG.debug the first message

                LOG.debug(m.strip())
            messages_string.extend('1M')
            messages_string.extend(bytearray(struct.pack('!I',
                                                         self.sequence)))
            messages_string.extend(bytearray(struct.pack('!I', len(m))))
            messages_string.extend(m)
            self.sequence += 1
        len_to_send = len(messages_string)
        len_sent = 0
        while len_sent < len_to_send:
            t = time.time() * 1000
            LOG.debug(
                'About to write to the socket (already sent %d out of %d '
                'bytes)' % (len_sent, len_to_send))
            written = s.write(buffer(messages_string, len_sent))
            write_time = time.time() * 1000 - t
            LOG.debug('Written %d bytes to socket in %f ms'
                         % (written, write_time))
            if written == 0:
                raise RuntimeError('socket connection broken')
                self.close()
                return False
            len_sent += written
        LOG.debug('Waiting for response from mtgraphite server')
        chunk = s.read(6)  # Expecting "1A"+4byte_num_of_metrics_received
        code = bytearray(chunk)[:2]
        LOG.debug('MTGraphite server response of %s'
                     % bytearray(chunk).strip())
        if code == '1A':
            LOG.info('Confirmed write to mtgraphite socket.')
        return True

    def _write_messages(self, msgset, max_emit_retries=10):
        msg_sent = False
        retries = 0
        while not msg_sent and retries <= max_emit_retries:
            try:
                retries += 1
                self._write_messages_no_retries(msgset)
                msg_sent = True
            except Exception as e:
                if retries <= max_emit_retries:

                    # Wait for (2^retries * 100) milliseconds

                    wait_time = 2.0 ** retries * 0.1
                    LOG.error(
                        'Could not connect to the mtgraphite server.Retry in '
                        '%f seconds.' % wait_time)

                    # The connection will be created again by
                    # _write_messages_no_retries().

                    self.close()
                    time.sleep(wait_time)
                else:
                    LOG.error('Bail out on sending to mtgraphite server'
                                 )
                    raise

    #################
    #   Public API  #
    #################

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception as e:
                LOG.exception(e)
            self.conn = None

    def send_messages(self, messages):
        """
        Helper method to send produce requests
        @param: *messages, one or more message payloads -- type str
        @returns: # of messages sent
        raises on error
        """

        # Guarantee that messages is actually a list or tuple (should always be
        # true)

        if not isinstance(messages, (list, tuple)):
            raise TypeError('messages is not a list or tuple!')

        # Raise TypeError if any message is not encoded as a str

        for m in messages:
            if not isinstance(m, str):
                raise TypeError('all produce message payloads must be type str'
                                )

        LOG.debug("""""")
        LOG.debug('New message:')
        LOG.debug('len(msgset)=%d, batch_every_n=%d, time=%d, '
                     'next_timeout=%d' % (len(self.msgset),
                                          self.batch_send_every_n,
                                          time.time(),
                                          self.next_timeout))
        if len(messages) > 0:
            self.msgset.extend(messages)
        if len(self.msgset) > self.batch_send_every_n or time.time() \
                > self.next_timeout:
             self._write_messages(self.msgset)
             self.msgset = []
             self.next_timeout = time.time() + self.batch_send_every_t

        return len(messages)



