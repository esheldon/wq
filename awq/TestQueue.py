#!/usr/bin/env python

import server

a=server.Cluster('bnl_desc.txt')
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
req['in_group']=[]
req['not_in_group']=[]
req['N']=6

print '-----------'

j = server.Job ({'require':req},a)
print j.cluster.Status()
j.match()
print j['status']
print j.cluster.Status()

print '-----------'

j2 = server.Job ({'require':req},a)
j2.match()
print j2['status']
print j2.cluster.Status()


print '-----------'

req={}
req['submit_mode']='bynode'
req['in_group']=['gen4']
req['not_in_group']=[]
req['min_cores']=0
req['N']=6
j3 = server.Job ({'require':req},a)
j3.match()
print j3['status']
print j3.cluster.Status()


print '-----------'

req={}
req['submit_mode']='exactnode'
req['node']='astro0001'
req['N']=10
j4 = server.Job ({'require':req},a)
j4.match()
print j4['status']
print j4.cluster.Status()

print '-----------'

j5 = server.Job ({'require':req},a)
j5.match()

print j4['status'], j5['status']
j4.unmatch()
j5.match()
print j4['status'], j5['status']
print j5.cluster.Status()
print '--------------'



req={}
req['submit_mode']='bygrp'
req['in_group']=['gen4']
req['not_in_group']=[]
req['min_cores']=0
req['N']=6
j6 = server.Job ({'require':req},a)
j6.match()
print j6['status']
print j6.cluster.Status()



for x in [j,j2,j3,j4,j5]:
    x.unmatch()
print j.cluster.Status()

j6.match()
print j6['status']
print j6.cluster.Status()
