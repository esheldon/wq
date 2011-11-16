#!/usr/bin/env python

import server

cluster = server.Cluster('bnl_desc.txt')

# print a.Status()
# a.Reserve(['astro0001','astro0001','astro0012'])
# print a.Status()
# a.Reserve(['astro0001','astro0001','astro0012'])
# print a.Status()
# a.Unreserve(['astro0001','astro0001','astro0012'])
# a.Unreserve(['astro0001','astro0001','astro0012'])
# print a.Status()

req={}
req['submit_mode']='bycore'
req['N']=6

print '-----------'

j = server.Job ({'require':req,'pid':0})
print cluster.Status()
j.match(cluster)
print j['status']
print cluster.Status()

print '-----------'

j2 = server.Job ({'require':req,'pid':1})
j2.match(cluster)
print j2['status']
print cluster.Status()


print '-----------'

req={}
req['submit_mode']='bynode'
req['in_group']=['gen4']
req['not_in_group']=[]
req['min_cores']=0
req['N']=6
j3 = server.Job ({'require':req,'pid':2})
j3.match(cluster)
print j3['status']
print cluster.Status()


print '-----------'

req={}
req['submit_mode']='byhost'
req['host']='astro0001'
req['N']=10
j4 = server.Job ({'require':req,'pid':3})
j4.match(cluster)
print j4['status']
print cluster.Status()

print '-----------'

j5 = server.Job ({'require':req,'pid':4})
j5.match(cluster)

print j4['status'], j5['status']
j4.unmatch(cluster)
j5.match(cluster)
print j4['status'], j5['status']
print cluster.Status()
print '--------------'


for x in [j,j2,j3,j4,j5]:
    x.unmatch(cluster)
print cluster.Status()

