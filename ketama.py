#!/usr/bin/python
#-*- coding:utf-8 -*-

__author__ = "iruca21"

"""
Ketama library compatible with implementation of spymemcached
(https://github.com/couchbase/spymemcached)
"""

import hashlib
from bisect import bisect


class KetamaHashRing:

	def __init__(self, hostname_or_ip_list, port, n_repetition=160):
		"""
		Create a hash ring using Ketama
                Args:
                  hostname_or_ip_list: list of hostnames or ip_address strings of nodes
                  port: port number to connect to nodes
                  n_repetition: repetition number used for ketama algorithm.
                    you don't need to change this value if you want spymemcache-compatibility
		"""
		# [(hashValue1, hostname1),...]
		hash_node_tuple_list = self.create_ketama_nodes( hostname_or_ip_list, port, n_repetition )

		# [hostname1,  hostname2,  hostname3,... ] ascending
		self.node_hostname_list = [ host for hash_key, host in hash_node_tuple_list ]
		# [hashValue1, hashValue2, hashValue3,... ] ascending
		self.hash_key_index_list = [ hash_key for hash_key, host in hash_node_tuple_list ]


	def get_node_for_key(self, key):
		"""
		get hostname or ip of the target node for the key string
		"""
		hash_value = self.ketama_hash( key )

		# get ceiling key
		
		# hash value for the key is larger than tha maximum hash key for the nodes,...
		if hash_value > self.hash_key_index_list[-1]:
			# because this is hash "ring"
			return self.node_hostname_list[0]

		return self.node_hostname_list[ bisect( self.hash_key_index_list, hash_value-1 ) ] # -1 for being inclusive


	def ketama_hash(self, key ):
		"""
		calculate the hash value (number) from key (string).
		"""
		#md5sum string
		md5sum = hashlib.md5(key).digest()
		
		# to byte array
		bKey = bytearray() 
		bKey.extend( md5sum )
	
		# do something special
		rv = ((bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16) | ((long) (bKey[1] & 0xFF) << 8) | (bKey[0] & 0xFF)
	        return rv & 0xffffffffL



	def get_key_for_node(self, hostname_or_ip, port, i_repetition ):
		"""
		get internal string for key of ketama hash...
	
		get_key_for_node( "example.com", 11211, 123 )
			-> will return "example.com/12.34.56.78:11211-123"
	
		get_key_for_node( "12.34.56.78", 3456, 78 )
			-> will return "12.34.56.78:3465-78"
		"""
		import socket
		import re
	
		regex = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
		result = regex.match( hostname_or_ip )
		
		if not result:
			# hostname_or_ip is hostname
			try:
				hostname = hostname_or_ip
				ip_string = socket.gethostbyname(hostname)
				return "%s/%s:%s-%s" % ( hostname, ip_string, port, i_repetition)
			except:
				raise KeyException("cannot resolve hostname. hostname="+ hostname )
	
		else:
			# hostname_or_ip is ip
			return "%s:%s-%s" % ( hostname_or_ip, port, i_repetition )


	def create_ketama_nodes(self, host_list, port, n_repetition=160 ):
		"""
		create Node Locator from hostname list and port number.
		if you use ketama for memcached, port will be 11211.
		"""

		hash_list = []
		node_list = []
	
		for host_or_ip in host_list:
			for i in range(0, n_repetition / 4 ):
				#md5sum string
				md5sum = hashlib.md5(self.get_key_for_node(host_or_ip, port, i)).digest()
				
				# to byte array
				bKey = bytearray() 
				bKey.extend( md5sum )
				
				for k in range(0, 4):
					hash_value = ((bKey[3 + k * 4] & 0xFF) << 24) | ((bKey[2 + k * 4] & 0xFF) << 16) | ((bKey[1 + k * 4] & 0xFF) << 8) | (bKey[k * 4] & 0xFF)
					hash_list.append( hash_value )
					node_list.append( host_or_ip )
		
		# [(hashValue, node_hostname),...]
		hash_node_tuple_list = zip( hash_list, node_list )
	
		hash_node_tuple_list.sort()
	
		return hash_node_tuple_list


if __name__ == "__main__":
	ketama_nodes = KetamaHashRing(["example.com", "yahoo.com", "google.com"], 11211)

	print ketama_nodes.get_node_for_key( "cachekey1" )
	print ketama_nodes.get_node_for_key( "cachekey2" )
	print ketama_nodes.get_node_for_key( "cachekey2" )
