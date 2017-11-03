#coding=utf-8,
#对已经存储的数据进行更新
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

DB_NAME="meituan"
COLL_NAME="Hotel_Info"
DB_CITYNAME="city_info"
COLL_CITYNAME="City"
SPIDER_LIMIT_DEAFAULT=500

#利用城市id和区域id获取酒店
TARGET_URL_SECINFLIMIT="http://ihotel.meituan.com/hbsearch/HotelSearch?newcate=1&cateId=20&uuid=2CA870DBC8EBBDC5E64BE5609965D839191A6EB0E07AA179C86A1C9421E869B6&attr_28=129&limit="
TARGET_URL_SECINFCITYID="&offset=500&cityId="
TARGET_URL_SECINFAREAID="&mypos=39.975792%2C116.488594&sort=defaults&endDay=20170724&startDay=20170724&lng=116.488594&lat=39.975792&areaId="
TARGET_URL_SECINFTAIL="&sourceType=hotel&client=iphone&utm_medium=WEIXINPROGRAM&utm_term=8.2.0&version_name=8.2.0&utm_campaign=entry%253DMTLive"


#获取城市的区域信息：变量cityid是城市id，数据来源与基础类
TARGET_CITY_AREAINFO="http://ihotel.meituan.com/group/v2/area/list?client=android&utm_medium=WEIXINPROGRAM&utm_term=999.9&utm_campaign=entry%253DMTLive&version_name=999.9&ci=42&uuid=2CA870DBC8EBBDC5E64BE5609965D839191A6EB0E07AA179C86A1C9421E869B6&cityId="

#获取每家酒店主要数据的接口(主要信息)
TARGET_URL_MAININFO_pre="http://ihotel.meituan.com/group/v1/yf/list/"
TARGET_URL_MAININFO_later="?_token=eJxVittugkAURf9lXkvCDDDD4JtysQJFQZSi8UEREZT7TW3678XUhzbZJ2vvk%2FUFqtkRjBCEkKcMaOqhYwgpJRDxSMIMCP7%2FeE5kwKFaK2C05QlkMBF3z4cz7K3IQQYhXtgxfyonDHk6s0EB57wJr2yxj8KaLfL4eYABvztthoBBTd1BHXh5cf9i82IdRxkYgVDvr8kHNMax6o8ter1PLdq2Vdn7c6JeNNbRZN7Tcl0RS7OEWsdLUrDBseYKC1lRJ0t1MdZcz7LPpkdnjmsVAXvFvZdIc9bcszNjpSNJyS25CdxD50vu%2Bw3rZBWEhHOo0aXoYFbH0GlDtjiqC8Heo2DdVEUn0U%2FHz1h7jk45Vo1GkzF368%2BZ64W%2BYPn4dtGNh5vYyyW54483OUJOycOobOuNt%2FLlULAmvXTaiEo6JUWCiK6ts4kQcfTxwBfw%2FQOJo4D7&start=1500854400000&end=1500940800000&type=0&poi=2576324&uuid=2CA870DBC8EBBDC5E64BE5609965D839191A6EB0E07AA179C86A1C9421E869B6&utm_medium=WEIXINPROGRAM&version_name=8.2.0&userid=&utm_campaign=AgroupBentry%253DMTLive"

#获取酒店的位置信息 参数为酒店id poiid
TARGET_URL_LOCATION_pre="http://ihotel.meituan.com/group/v1/poi/"
TARGET_URL_LOCATION_later="?start=1501545600000&end=1501632000000&mypos=39.975716,116.488594&cityId=1&subtype=0&type=1&recType=0&isRecommand=0&isLocal=1&entryType=2&uuid=1A6E888B4A4B29B16FBA1299108DBE9CB467F3C3B92F2C2406E67162DC839907&fields=phone,markNumbers,cityId,addr,lng,hasGroup,subwayStationId,cates,frontImg,chooseSitting,wifi,avgPrice,style,featureMenus,avgScore,name,parkingInfo,lat,introduction,showType,areaId,preferent,lowestPrice,cateName,areaName,sourceType,geo,fodderInfo,scoreSource,brandId&city_id=1&poi_id=6829413&locate_city_id=1&client=iphone&client_type&utm_term=999.9&utm_medium=WEIXINPROGRAM&version_name=999.9&utm_campaign=entry%3DMTHotel&utm_content&utm_source"

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



class Meituan_spider(object):

	def __init__(self,sec_url):
		self.sec_url=sec_url

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
		response=requests.get(TARGET_URL_MAININFO_pre+str(poiid)+TARGET_URL_MAININFO_later)
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


#测试获取城市区域信息函数
def test_area():
	meituan=Meituan_mongo(host='127.0.0.1',port=27017,db_name="city_info",coll_name="City")
	spider=Meituan_spider(TARGET_URL_SECINFTAIL)
	#d=spider.get_meituan_areainfo(42,"西安")
	#meituan.insert_doc(d)
	#meituan.insert_docs(spider.get_meituan_secdatas())
	s=cityid.City_Info().city_info
	for key in s:
		try:
			d=spider.get_meituan_areainfo(s[key],key)
			meituan.insert_doc(d)
		except Exception,e:
			pass

#爬虫信息同步到数据库中 测试代码
def test_hotel():
	meituan=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_NAME,coll_name=COLL_NAME)
	spider=Meituan_spider(TARGET_URL_SECINFTAIL)

	r=spider.get_meituan_secdatas(1,2073,1000,"北京市","平谷区")
	meituan.insert_docs(r)

def auto_sync(city_id):
	#查询实例
	meituan_query=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_CITYNAME,coll_name=COLL_CITYNAME)
	#插入数据实例
	meituan_insert=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_NAME,coll_name=COLL_NAME)
	spider=Meituan_spider(TARGET_URL_SECINFTAIL)
	data={'city_id':city_id}
	obj=meituan_query.query_doc(data)
	if not len(obj['list_area']):
		print "该城市没有找到区域"
	city_name=obj['city_name']
	for item in obj['list_area']:
		area_id=item['id']
		area_name=item['name']
		try:
			r=spider.get_meituan_secdatas(city_id,area_id,SPIDER_LIMIT_DEAFAULT,city_name,area_name)
			meituan_insert.insert_docs(r)
		except Exception,e:
			#logging.INFO("city_id为 "+str(city_id)+" 发送异常"+e)
			print e

#对日志进行异常记录，对存在异常的接口进行test手动录入
def test(city_id):
	#查询实例
	meituan_query=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_CITYNAME,coll_name=COLL_CITYNAME)
	#插入数据实例
	meituan_insert=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_NAME,coll_name=COLL_NAME)
	spider=Meituan_spider(TARGET_URL_SECINFTAIL)
	data={'city_id':city_id}
	obj=meituan_query.query_doc(data)
	if not len(obj['list_area']):
		print "该城市没有找到区域"
	city_name="广州"
	area_id=274
	area_name="番禺区"

	try:
		r=spider.get_meituan_secdatas(city_id,area_id,SPIDER_LIMIT_DEAFAULT,city_name,area_name)
		meituan_insert.insert_docs(r)
	except Exception,e:
		#logging.INFO("city_id为 "+str(city_id)+" 发送异常"+e)
		print e

#对异常日志进行分析处理，批量完成
def auto_except_test(city_id,area_id):
	#查询实例
	meituan_query=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_CITYNAME,coll_name=COLL_CITYNAME)
	#插入数据实例
	meituan_insert=Meituan_mongo(host='127.0.0.1',port=27017,db_name=DB_NAME,coll_name=COLL_NAME)
	spider=Meituan_spider(TARGET_URL_SECINFTAIL)
	data={'city_id':city_id}
	obj=meituan_query.query_doc(data)
	if not len(obj['list_area']):
		print "该城市没有找到区域"
	city_name=obj['city_name']
	for item in obj['list_area']:
		if item['id']==area_id:
			area_name=item['name']
			break
	r=spider.get_meituan_secdatas(city_id,area_id,SPIDER_LIMIT_DEAFAULT,city_name,area_name)
	meituan_insert.insert_docs(r)


#Python多进程进行全国酒店爬虫
def start_auto_sync(list_ss):
	for city_id in list_ss:
		auto_sync(city_id)
		

if __name__=='__main__':
	pass
	



	
	
	
	

	

	


	

	




	










