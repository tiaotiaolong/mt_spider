#coding=utf-8,
#爬取美团酒店的相关数据,对数据进行筛选，处理，进行存储，存储到本机的mongodb上。
#author:跳跳龙
import pymongo
from pymongo import MongoClient
import datetime
import sys
import requests
import json
import time
import os
from multiprocessing import Pool
import time
import threading
import Queue
#封装的美团的城市信息
import cityid
import datacity_id

DB_NAME="meituan"
COLL_NAME="Hotel_Info"
DB_CITYNAME="city_info"
COLL_CITYNAME="City"
COLL_NAME_UPDATE="hotel"
SPIDER_LIMIT_DEAFAULT=500
HOST_NAME="localhost"
PORT=27017
COLL_NAME_PROCESS='process'
SPIDER_PROCESS=0


#利用城市id和区域id获取酒店
TARGET_URL_SECINFLIMIT="http://ihotel.meituan.com/hbsearch/HotelSearch?newcate=1&cateId=20&uuid=2CA870DBC8EBBDC5E64BE5609965D839191A6EB0E07AA179C86A1C9421E869B6&attr_28=129&limit="
TARGET_URL_SECINFCITYID="&offset=500&cityId="
TARGET_URL_SECINFAREAID="&mypos=39.975792%2C116.488594&sort=defaults&endDay=20170724&startDay=20170724&lng=116.488594&lat=39.975792&areaId="
TARGET_URL_SECINFTAIL="&sourceType=hotel&client=iphone&utm_medium=WEIXINPROGRAM&utm_term=8.2.0&version_name=8.2.0&utm_campaign=entry%253DMTLive"


#获取城市的区域信息：变量cityid是城市id，数据来源与基础类
TARGET_CITY_AREAINFO="http://ihotel.meituan.com/group/v2/area/list?client=android&utm_medium=WEIXINPROGRAM&utm_term=999.9&utm_campaign=entry%253DMTLive&version_name=999.9&ci=42&uuid=2CA870DBC8EBBDC5E64BE5609965D839191A6EB0E07AA179C86A1C9421E869B6&cityId="

#获取每家酒店主要数据的接口(主要信息)
TARGET_URL_MAININFO_pre="http://ihotel.meituan.com/group/v1/yf/list/{0}?start={1}&end={2}&type=0&utm_medium=WEIXINPROGRAM&version_name=8.2.0&utm_campaign=AgroupBentry%3DMTHotel&userid=13242&utm_campaign=AgroupBentry%3DMTHotel"
#TARGET_URL_MAININFO_later=

#获取酒店的位置信息 参数为酒店id poiid
TARGET_URL_LOCATION_pre="http://ihotel.meituan.com/group/v1/poi/"
TARGET_URL_LOCATION_later="?start=1501545600000&end=1501632000000&mypos=39.975716,116.488594&cityId=1&subtype=0&type=1&recType=0&isRecommand=0&isLocal=1&entryType=2&uuid=1A6E888B4A4B29B16FBA1299108DBE9CB467F3C3B92F2C2406E67162DC839907&fields=phone,markNumbers,cityId,addr,lng,hasGroup,subwayStationId,cates,frontImg,chooseSitting,wifi,avgPrice,style,featureMenus,avgScore,name,parkingInfo,lat,introduction,showType,areaId,preferent,lowestPrice,cateName,areaName,sourceType,geo,fodderInfo,scoreSource,brandId&city_id=1&poi_id=6829413&locate_city_id=1&client=iphone&client_type&utm_term=999.9&utm_medium=WEIXINPROGRAM&version_name=999.9&utm_campaign=entry%3DMTHotel&utm_content&utm_source"

#线程停止执行标志
EXIT_FLAG=False

#默认开启线程数
THREADS_NUM=20

class Meituan_mongo(object):
	def __init__(self,host,port,db_name,coll_name):
		self.host=host
		self.port=port
		self.db_name=db_name
		self.coll_name=coll_name
		client=pymongo.MongoClient(host=self.host,port=self.port)
		self.db_instance=client[db_name]
		self.coll_instance=self.db_instance[coll_name]

	def insert_doc(self,data):
		if not isinstance(data,dict):
			print"待插入的document类型有误"
			sys.exit(1)

		self.coll_instance.insert_one(data)
		print"[+]成功的插入了一个Document"

	def insert_docs(self,datas):
		if not isinstance(datas,list):
			print"待插入的document组类型有误"
			sys.exit(1)

		self.coll_instance.insert(datas)
		print"[+]成功的插入了一组Documents"

	#data为查询字典
	def query_doc(self,data):
		return self.coll_instance.find_one(data)

	#查询集合中的所有文档
	def query_all(self):
		return self.coll_instance.find({})		

	def update_doc(self,pre_data,data):
		self.coll_instance.update_one(pre_data,data)


#用来记录爬虫的抓取进度
class Meituan_mongo_process(object):
	def __init__(self,host,port,db_name,coll_name):
		self.host=host
		self.port=port
		self.db_name=db_name
		self.coll_name=coll_name
		client=pymongo.MongoClient(host=self.host,port=self.port)
		self.db_instance=client[db_name]
		self.coll_instance=self.db_instance[coll_name]

	#用来计算爬取进度，爬取一条在数据库中改写一条，
	def update_process(self,pre_data,data):
		self.coll_instance.update_one(pre_data,data)

	#查询集合中的所有文档
	def query_all(self):
		return self.coll_instance.find({})	

	def insert_doc(self,data):
		self.coll_instance.insert_one(data)
		



class Meituan_spider(object):

	def __init__(self,sec_url):
		self.sec_url=sec_url
		self.now=int(time.time())

	#获取美团所有酒店信息(次要数据)
	def get_meituan_secdatas(self,city_id,area_id,limit_num,city_name,area_name):
		
		response=requests.get(TARGET_URL_SECINFLIMIT+str(limit_num)+TARGET_URL_SECINFCITYID+str(city_id)+TARGET_URL_SECINFAREAID+str(area_id)+TARGET_URL_SECINFTAIL)
		#本次返回的所有酒店的列表

		if json.loads(response.text).has_key('error'):
			self.exception_logging(city_id,area_id)
			
			
		list_Hotelobject=json.loads(response.text)['data']['searchresult']

		#数据被处理之后的酒店信息列表
		list_result=[]
		#用来装载每个酒店的信息字典
		dic_temp={}
		for i in range(len(list_Hotelobject)):
			dic_temp['area_id']=area_id
			dic_temp['city_id']=city_id
			dic_temp['city_name']=city_name
			dic_temp['area_name']=area_name
			#每家酒店的唯一id，在详细信息检索时作为索引
			dic_temp['poiid']=list_Hotelobject[i]['poiid']
			#每家酒店的名字
			dic_temp['name']=list_Hotelobject[i]['name']
			#每家酒店的最低价
			dic_temp['lowestPrice']=list_Hotelobject[i]['lowestPrice']
			#每家酒店的具体地址
			dic_temp['addr']=list_Hotelobject[i]['addr']
			#每家酒店的评分信息
			dic_temp['scoreIntro']=list_Hotelobject[i]['scoreIntro']
			#获取这家酒店的主要信息列表
			print "[+]"+str(list_Hotelobject[i]['poiid'])+" 酒店已成功抓取到......"
			dic_temp['HotelmainInfo']=self.get_meituan_maindatas(dic_temp['poiid'])

			list_result.append(dic_temp)
			dic_temp={}	
		return list_result	

	#获取美团每家酒店主要信息(主要数据)
	def get_meituan_maindatas(self,poiid):
		#print TARGET_URL_MAININFO_pre.format(str(poiid),self.now*1000,(self.now+86400)*1000)
		response=requests.get(TARGET_URL_MAININFO_pre.format(str(poiid),self.now*1000,(self.now+86400)*1000))
		list_Hotel_maininfo=json.loads(response.text)['data']['result']
		list_result=[]
		dic_temp={}
		for i in range(len(list_Hotel_maininfo)):
			#酒店的每种类型的名字
			dic_temp['goodsName']=list_Hotel_maininfo[i]['goodsName']
			#该类型的价格
			dic_temp['averagePrice']=list_Hotel_maininfo[i]['averagePrice']
			#酒店的每种类型的id
			dic_temp['goodsId']=list_Hotel_maininfo[i]['goodsId']
			#是否可预订，(d+),代表可预订，(d+),代表不能预订
			dic_temp['goodsStatus']=list_Hotel_maininfo[i]['goodsStatus']

			list_result.append(dic_temp)
			dic_temp={}	
		return list_result

	#获取美团城市的区域信息 返回值是字典
	def get_meituan_areainfo(self,cityid,city_name):
		response=requests.get(TARGET_CITY_AREAINFO+str(cityid))
		list_AreaInfo=json.loads(response.text)['data']['areasinfo']
		list_result=[]
		dic_temp={}
		for i in range(len(list_AreaInfo)):
			#获取区域的id
			dic_temp['id']=list_AreaInfo[i]['id']
			#获取区域的中文名字
			dic_temp['name']=list_AreaInfo[i]['name']

			list_result.append(dic_temp)
			dic_temp={}	
		#城市的区域信息表
		dic_result={}
		dic_result['city_id']=cityid
		dic_result['city_name']=city_name
		dic_result['list_area']=list_result
		return dic_result

	#获取美团酒店的地理位置信息:
	def get_meituan_location(self,poiid):
		response=requests.get(TARGET_URL_LOCATION_pre+str(poiid)+TARGET_URL_LOCATION_later)
		longitude=json.loads(response.text)['data']['lng']
		latitude=json.loads(response.text)['data']['lat']
		return (longitude,latitude)

	#异常处理日志记录
	def exception_logging(self,city_id,area_id):
		with open('/Users/tangtianlong/temp/logger.log','a') as f:
			#f.write('lalalallalal')
			f.write("在时间为 %s 时，爬虫在处理city_id为 %d ，area_id为 %d 的时候发送异常，请及时分析并处理......\n " % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),city_id,area_id))


#多线程实现更新
class SpiderThread(threading.Thread):
	def __init__(self,threadname,thread_queue,lock,meituan_mongo,meituan_spider,meituan_mongo_process):
		super(SpiderThread,self).__init__()
		self.threadname=threadname
		self.thread_queue=thread_queue
		self.lock=lock
		self.meituan_mongo=meituan_mongo
		self.meituan_spider=meituan_spider
		self.meituan_mongo_process=meituan_mongo_process

	def run(self):
		self.thread_task()
		
	#多线程任务实现方法
	def thread_task(self):
		global EXIT_FLAG
		while not EXIT_FLAG:
			self.lock.acquire()
			if not self.thread_queue.empty():
				poiid=self.thread_queue.get()
				#self.meituan_mongo_process.insert_doc({'process':1})
				global SPIDER_PROCESS
				SPIDER_PROCESS+=1
				#往数据库里记录一条，用来查询进度
				self.meituan_mongo_process.update_process({'process':self.meituan_mongo_process.query_all()[0]['process']},{'$set':{'process':SPIDER_PROCESS}})
				self.lock.release()
				temp_data=self.meituan_spider.get_meituan_maindatas(poiid)
				self.meituan_mongo.update_doc({'poiid':poiid},{'$set':{'HotelmainInfo':temp_data}})
				print "threadName: {0} poiid为 {1} 的酒店的业务信息更新成功".format(self.threadname,poiid)
			else:
				self.lock.release()


#负责定时更新数据的接口
def update_timing_maindata():
	#首先获取所有的poiid
	
	#申请一个多线程锁
	threadLock = threading.Lock()
	#申请一个meituan_mongo
	meituan_mongo=Meituan_mongo(HOST_NAME,PORT,DB_NAME,COLL_NAME_UPDATE)
	#poiid的全部数据
	poiid_all_list=[]
	#从数据库中读取poiid
	for item in meituan_mongo.query_all():
		poiid_all_list.append(item['poiid'])
	#申请一个meituan_spider
	meituan_spider=Meituan_spider(TARGET_URL_MAININFO_pre)

	#申请一个Meituan_mongo_process，用来后期进行进度查询
	meituan_mongo_process=Meituan_mongo_process(HOST_NAME,PORT,DB_NAME,COLL_NAME_PROCESS)


	quene_length=len(poiid_all_list)
	global workQueue
	workQueue = Queue.Queue(quene_length)
	queueLock = threading.Lock()
	threads = []
	
	for i in range(THREADS_NUM):
		thread=SpiderThread("Spider_thread-"+str(i),workQueue,queueLock,meituan_mongo,meituan_spider,meituan_mongo_process)
		thread.start()
		threads.append(thread)

	queueLock.acquire()
	for item in poiid_all_list:
		workQueue.put(item)
	queueLock.release()

	while not workQueue.empty():
		pass

	#设置线程为真时退出
	global EXIT_FLAG
	EXIT_FLAG=True
	for thread in threads:
		thread.join()
	print "所有线程执行完毕，数据更新完毕"
	










	




	
	

	

	


	

	




	










