# aliyun-tablestore-helper

阿里云 tablestore 辅助方法
```
const  {TSHelper,TSSet,TSGet,TSRange,TSCreate} = require("aliyun-tablestore-helper");
```
## 配置参数

本地创建 ath.yaml ，内容：
```
accessKeyId: 'xxxxxxxx'
secretAccessKey: 'xxxxxxxxxx'
endpoint: 'https://xxxxxxxx.ots.aliyuncs.com'
instancename: 'xxxxxx'
maxRetries: 20
```
## 创建tablestore表
```
TSCreate.reset("abc",{hash:String},function (err, data) {})
```

## 表操作

### 向tablestore写数据
```
TSSet.select("abc").put({"a":1},{"b":2})
.attr({"haha":1},{"hehe":2})
.update({"x":9},{"y":8},{"z":7})
.attr({"haha":"1","hehe":"2"});

TSSet.exec(function(err,data){
	callback(null, TSHelper.response(data));
});
```
### 向tablestore读数据
```
TSGet.select("abc")
.get({"a":1},{"b":2})
.get({"x":9},{"y":8},{"z":7})
.attr({"haha":"1"},{"heihei":3})
.select("def")
.get({"hello":1234567});

TSGet.exec(function(err,data){
    data=TSHelper.parse(data);
	callback(null, TSHelper.response(data));
});
```
### 从tablestore读取范围数据
```
TSRange.select("abc")
.range("hash")
.equal({"status":"complete"});

TSRange.exec(function (err, data) {
    data=TSHelper.parse(data);
	TSHelper.parseRange(data);
})
```

