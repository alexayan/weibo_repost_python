#encoding: utf-8
#author : yanSun
#mail   : 773655223@qq.com

from urllib2 import urlopen, quote, URLError
import json, time, copy, os, sys
from datetime import datetime


#构造url
def create_url(url, **args):
	def parse_url(url):
		o = {}
		index = url.find("?")
		if index == -1:
			o["main"] = url
			o["args"] = {}
		else:
			main = url[:index]
			args = {}
			temp = url[index+1:].split("&")
			for i in temp:
				pair = i.split("=")
				args[pair[0]] = pair[1]
			o["main"] = main
			o["args"] = args
		return o
	def merge_obj(s, t):
		for i in t.keys():
			s[i] = t[i]
	o = parse_url(url)
	merge_obj(o["args"], args)
	url = o["main"] + "?"
	for i in o["args"].keys():
		url = url + i + "=" + str(o["args"][i]) + "&"
	return url[:len(url)-1]

def parse_obj_with_opts(obj, *opts):
	def get_value(obj, path):
		arr = path.split(".")
		for i in arr:
			obj = obj[i]
		return obj
	res = {}
	for opt in opts:
		res[opt] = get_value(obj, opt)
	return res

class TokenError(Exception):
	def __init__(self, m):
		m = "{'type':'token_error', 'error_message': '"+m+"'}"
		Exception.__init__(self, m)

class RepostError(Exception):
	def __init__(self, m):
		m = "{'type':'repost_error', 'error_message': '"+m+"'}"
		Exception.__init__(self, m)

class ConnectionError(Exception):
	def __init__(self, m):
		m = "{'type':'connection_error', 'error_message': '"+m+"'}"
		Exception.__init__(self, m)

#access_token管理类
#由于新浪api存在调用频率限制，每一个access_token的频率为 150 次/小时。因此提供多个access_token以增加调用次数
class TokensManager:
	def __init__(self, tokens):
		self.tokens = tokens
		self.status = {}
		self.update_status()

	def update_status(self):
		for i in self.tokens:
			res = Api.rate_limit_status(i)
			self.status[i] = res
		dates = []
		for j in self.status.values():
			dates.append(j["time"])
		self.time = min(dates) #access_token使用次数刷新的最早时间，通过与当前时间相减，可以算出需要等待的时间
		for i in self.tokens:
			if self.status[i]["hits"] != 0:
				self.live = True
				return
		self.live = False

	#返回可用的access_token,当所有access_token都不可用时，抛出异常
	def get_token(self):
		if not self.live:
			self.update_status()
		for i in self.tokens:
			if self.status[i]["hits"] != 0:
				self.status[i]["hits"] -= 1
				return i
			elif self.status[i]["time"] <= datetime.now():
				self.status[i] = Api.rate_limit_status(i)
				self.status[i]["hits"] -= 1
				return i
		dates = []
		for j in self.status.values():
			dates.append(j["time"])
		self.time = min(dates)
		self.live = False
		raise TokenError("token error: "+"relive in:"+self.time.strftime("%Y-%m-%d %H:%M:%S"))

#api管理类
class Api:
	def __init__(self, token_manager):
		self.token_manager = token_manager

	def repost_timeline(self, id, count=200, page=1, since_id=0, max_id=0, filter_by_author=0, base=None):
		def parse_repost_timeline_data(resp):
			res = []
			for i in resp["reposts"]:
				res.append(i)
			return res
		res = []
		if base:
			main = base
		else:
			main = "https://api.weibo.com/2/statuses/repost_timeline.json"
		url = create_url(main, id=id, access_token=self.token_manager.get_token(), count=count, page=page, since_id=since_id, max_id=max_id, filter_by_author=filter_by_author)
		while True:
			try:
				print "request: "+url
				resp = urlopen(url).read()
				resp = json.loads(resp)
				print "process"
				if resp["reposts"] == []:
					print "done"
					break
			except Exception as e:
				print e
				print "done"
				break
			res.extend(parse_repost_timeline_data(resp))
			page = page + 1
			url = create_url(url, page=page, access_token=self.token_manager.get_token())
		return res

	#def repost_timeline_hack(self, id, count=200, page=1, since_id=0, max_id=0, filter_by_author=0, base=None):
	#	def parse_args(obj):
	#		s = ""
	#		for i in obj.keys():
	#			s = s + i + "=" + str(obj[i]) + "&"
	#		return s[:len(s)-1]
	#	res = []
	#	main = "http://open.weibo.com/tools/aj_interface.php"
	#	if not base:
	#		base = "https://api.weibo.com/2/statuses/repost_timeline.json"
	#	args = {"count":count, "page":page, "max_id":max_id, "filter_by_author":filter_by_author, "id":id, "access_token":"2.0077Oy5CciavOB011e45245blKTfcD"}
	#	o = "api_url="+quote(base)+"&request_type=get&_t=0&request_data="+quote(parse_args(args))
	#	while True:
	#		print o
	#		resp = urlopen(main, o).read()
	#		print json.loads(resp)
	#		resp = json.loads(resp)["retjson"]
	#		if type(resp) == str:
	#			break
	#		res.extend(parse_repost_timeline_data(resp))
	#		args["page"] = args["page"] + 1
	#		o = "api_url="+quote(base)+"&request_type=get&request_data="+quote(parse_args(args))
	#	return res

	@staticmethod
	def rate_limit_status(access_token):
		url = "https://api.weibo.com/2/account/rate_limit_status.json"
		url = create_url(url, access_token=access_token)
		try:
			resp = json.loads(urlopen(url).read())
		except Exception as e:
			raise ConnectionError("rate_limit_status : can't connect the server")
		remaining_user_hits = resp["remaining_user_hits"]
		reset_time = datetime.strptime(resp["reset_time"], "%Y-%m-%d %H:%M:%S")
		return {"hits":remaining_user_hits, "time": reset_time}

	#http://apps.weibo.com/tudou_app
	@classmethod
	def get_api(cls):
		if not getattr(cls, "api", None):
			cls.api = Api(token_manager=TokensManager(["2.0077Oy5CciavOB011e45245blKTfcD","2.00wvX9cFstlp3D8d74f72f27g_dpdB","2.00wvX9cFciavOB32995bad1a0pEx8U","2.00wvX9cFDGGdjC01a76263a8Vlf4YC"]))
		return cls.api

class User:
	def __init__(self, **args):
		for i in args.keys():
			setattr(self, i, args[i])

#reposts: 转发当前微博的微博对象列表
#parent：当前微博转发的微博对象
#src：微博转发关系的源，最远的祖先
#其他属性与新浪接口返回微博属性一致，如id, mid....
#to do: 由于保存时是保存json属性，因此设置新属性时需要更新json的状态
class Weibo:
	def __init__(self, parent=None, src=None, **args):
		self.reposts = []
		self.parent = parent
		self.src = self
		args.pop("retweeted_status", None)
		for i in args.keys():
			setattr(self, i, args[i])
		self.json = args
		if getattr(self, "user", None):
			self.user = User(**self.user)

	#获得当前微博的转发微博
	def get_reposts(self, **args):
		resp = Api.get_api().repost_timeline(id=self.id, **args)
		#resp = self.api.repost_timeline_hack(id=self.id, **args)
		for i in resp:
			w = Weibo(parent=self, src=self.src, **i)
			self.reposts.append(w)
		return self

	def show(self):
		print "mid: "+ str(self.id) + " user: " + str(self.user.id)

	def to_json(self):
		if self.reposts == []:
			return self.json
		reposts = []
		for i in self.reposts:
			reposts.append(i.to_json())
		obj = copy.deepcopy(self.json)
		obj["reposts"] = reposts
		return obj


#def get_reposts(core=["3712327649617383"], level=2):
#	for i in range(2):
#		core.append("hehe")
#		while len(core) != 0 and core[0] != "hehe":
#			id = core.pop(0)
#			w = Weibo(id=id).get_reposts()
#			for j in w.reposts:
#				core.append(j.id)
#			time.sleep(1.0)
#		core.remove("hehe")


#日志类，保存和恢复程序运行状态
class Log:
	def __init__(self, filename):
		self.filename = filename

	def save_repost_state(self, res):
		r = []
		if os.path.isfile(self.filename):
			os.remove(self.filename)
		f = file(self.filename, "w")
		for i in res:
			r.append(i.to_json())
		json.dump(r, f)
		f.close()

	def load_repost_state(self):
		def create(o, parent, src):
			w = Weibo(**o)
			t=  []
			if w.reposts == []:
				return w
			for j in w.reposts:
				t.append(create(j, w, src))
			w.reposts = t
			w.parent = parent
			w.src = src
			return w
		o = json.load(file(self.filename))
		res = []
		for i in o:
			res.append(create(i, None, i))
		return res

	def save(self, obj):
		if os.path.isfile(self.filename):
			os.remove(self.filename)
		f = file(self.filename, "w")
		json.dump(obj, f)
		f.close()

	def load(self):
		if not os.path.isfile(self.filename):
			print self.filename + " not found"
		return json.load(file(self.filename))

#获取微博转发关系链。
#core：种子微博id列表
#state_file：微博转发关系链保存的文件名
#position_file: 状态保存文件名
#level：获取转发的层数
def start(state_file, position_file, core=["3711909720982429"], level=2):
	res = []
	for i in range(len(core)):
		core[i] = Weibo(id=core[i])
		res.append(core[i])
	for j in range(level):
		print "process level:", str(j+1)
		core.append("hehe")
		while len(core) != 0 and core[0] != "hehe":
			try:
				temp = core.pop(0)
				ws = temp.get_reposts().reposts
			except (URLError, ConnectionError) as e:
				print e
				Log(state_file).save_repost_state(res)
				Log(position_file).save({"level": level, "cur_level": j+1})
				return
			except TokenError as e:
				print e
				Log(state_file).save_repost_state(res)
				Log(position_file).save({"level": level, "cur_level": j+1})
				print "sleep :" + str((Api.get_api().token_manager.time-datetime.now()).total_seconds()+60)+" seconds"
				time.sleep((Api.get_api().token_manager.time-datetime.now()).total_seconds()+60)
				try:
					ws = temp.get_reposts().reposts
				except Exception as e:
					print e
					return
			except Exception as e:
				print e
				Log(state_file).save_repost_state(res)
				Log(position_file).save({"level": level, "cur_level": j+1})
				return
			for k in ws:
				core.append(k)
		core.remove("hehe")
	Log(state_file).save_repost_state(res)
	Log(position_file).save({"level": level, "cur_level": j+1})
	return res

#从上次中断的状态恢复继续运行
#state_file: 微博转发关系链保存的文件名
#position_file: 状态保存文件名
#level : 获取转发的层数，如果设置则覆盖position_file文件中保存的level
def restart(state_file, position_file, level=None):
	pos = Log(position_file).load()
	cur_level = pos["cur_level"]
	if not level:
		level = pos["level"]
	o = Log(state_file).load_repost_state()
	res = copy.copy(o)
	t = []
	for i in range(cur_level-1):
		for j in o:
			t.extend(j.reposts)
		o = t
		t = []
	o.append("hehe")
	for i in o:
		if getattr(i, "reposts", None) != [] and getattr(i, "reposts", None):
			if cur_level != level:
				o.extend(i.reposts)
			o.remove(i)
	for i in range(level-cur_level+1):
		while len(o) != 0 and o[0] != "hehe":
			try:
				temp = o.pop(0)
				ws = temp.get_reposts().reposts
			except (URLError, ConnectionError) as e:
				print e
				Log(state_file).save_repost_state(res)
				Log(position_file).save({"level": level, "cur_level": cur_level+i})
				return
			except TokenError as e:
				print e.message
				Log(state_file).save_repost_state(res)
				Log(position_file).save({"level": level, "cur_level": cur_level+i})
				print "sleep :" + str((Api.get_api().token_manager.time-datetime.now()).total_seconds()+60)+" seconds"
				time.sleep((Api.get_api().token_manager.time-datetime.now()).total_seconds()+60)	
				try:
					ws = temp.get_reposts().reposts
				except Exception as e:
					print e
					return
			except Exception as e:
				print e
				Log(state_file).save_repost_state(res)
				Log(position_file).save({"level": level, "cur_level": cur_level+i})
				return
			for k in ws:
				o.append(k)
		o.remove("hehe")
	Log(state_file).save_repost_state(res)
	Log(position_file).save({"level": level, "cur_level": cur_level+i})
	return res

def show_relation(ifile, ofile):
	def show(o):
		f.write(str(o.id) + "\n")
		if len(o.reposts) == 0:
			return 
		t = []
		for i in o.reposts:
			t.append(str(i.id))
		f.write(str(t)+"\n")
		for i in o.reposts:
			show(i)
	o = Log(ifile).load_repost_state()
	f = file(ofile, "w")
	for i in o:
		show(i)
	f.close()

#种子3711909720982429
while True:
	template = """
输入操作类型： 
	a 代表采集微博转发关系
	b 代表从保存的状态继续采集微博转发关系
	c 代表将微博转发关系转换成特定的格式
	q 代表退出程序
注意： 输入确保被引号包括，如\"输入\"
	"""
	print template
	opt = input("请输入操作类型（a,b,c,q）:").lower()
	if opt == "a":
		print "当前位置：采集微博转发关系"
		sf = input("请输入微博转发数据保存文件(e:/weibo.txt)。如果文件不存在，则自动创建新文件。如果存在，请确保文件为空。\n请输入：")
		of = input("请输入微博数据采集状态保存文件(e:/weibo_1.txt)。如果文件不存在，则自动创建新文件。如果存在，请确保文件为空。\n请输入：")
		core = str(input("请输入微博种子id,多个种子以逗号(英文输入法)分隔（1233,2333,3333）。\n请输入："))
		core = core.replace(" ", "").split(",")
		level = int(input("请输入微博转发关系采集层数。\n请输入："))
		start(sf, of, core=core, level=level)
		print "成功"
		continue
	elif opt == "b":
		print "当前位置：微博采集进程恢复"
		sf = input("请输入微博转发数据保存文件(e:/weibo.txt)。\n请输入：")
		of = input("请输入微博数据采集状态保存文件(e:/weibo_1.txt)。\n请输入：")
		level = int(input("请输入重置微博转发关系采集层数。如果不需要重置，则输入0\n请输入："))
		if level == "0":
			level = None
		restart(sf, of, level=level)
		print "成功"
		continue
	elif opt == "c":
		print "当前位置：将微博转发关系转换成特定的格式"
		sf = input("请输入微博转发数据保存文件(e:/weibo.txt)。\n请输入：")
		of = input("请输入格式转换后保存文件(e:/weibo_1.txt)。\n请输入：")
		show_relation(sf, of)
		print "成功"
		continue
	elif opt == "q":
		break
