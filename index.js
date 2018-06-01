'use strict';

const _=require('lodash');
const TableStore = require("tablestore");
const Long = TableStore.Long;

const config = {
	accessKeyId: 'xxxxxxx',
	secretAccessKey: 'xxxxxxxxxxxxxx',
	endpoint: 'xxxxxxxx',
	instancename:'xxxxxxxx',
	maxRetries:20,//默认20次重试，可以省略这个参数。
};

const client=new TableStore.Client(config);
exports.client=client;


function TSHelper(){}
function TSGet(){}
function TSSet(){}
function TSRange(){}
function TSCreate(){}

TSCreate.prototype={
	constructor:TSCreate,
	create:function(tableName,pk,handler){
		//{goods_id:Number}
		// [{name: 'goods_id',type: 'INTEGER'}]
		let primary_key=_.map(pk,function (value, index) {
			if(value===Number)value="INTEGER";
			else if(value===String)value="STRING";
			return {name:index,type:value};
		})
		
		var params = {
			tableMeta: {
				tableName: tableName,
				primaryKey:primary_key
			},
			reservedThroughput: {
				capacityUnit: {
					read: 0,
					write: 0
				}
			},
			tableOptions: {
				timeToLive: -1,// 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
				maxVersions: 1// 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).
			}
		};
		client.createTable(params, handler);
	},
	reset:function(tableName,pk,handler){
		//先删除，后创建
		client.deleteTable({tableName: tableName},()=>{
			this.create(tableName,pk,handler);
		})
	}
}

TSGet.prototype={
	constructor:TSGet,
	tables:[],
	_currentTable:null,
	_currentRow:null,
	_handler:null,
	select:function(tableName){
		this._end();
		this._currentTable={tableName:tableName};
		return this;
	},
	get:function(...primary){
		if(!this._currentRow){
			this._currentRow={primaryKey:[]};
		}
		this._currentRow.primaryKey.push([...primary]);
		return this;
	},
	attr:function(...columns){
		var condition = new TableStore.CompositeCondition(TableStore.LogicalOperator.AND);
		_.each(columns,function(item,index){
			_.each(item,function(value,key){
				condition.addSubCondition(new TableStore.SingleColumnCondition(key, item[key], TableStore.ComparatorType.EQUAL));
			})
		})
		this._currentRow.columnFilter = condition;
		return this;
	},
	exec:function(handler){
		this._handler=handler;
		
		this._end();
		let tb=this.tables;
		this.tables=[];

		client.batchGetRow({tables:tb},this._handler);
	},
	trace:function(){
		this._end();
		let tb=this.tables;
		this.tables=[];
		return {tables:tb};
	},
	_endRow:function(){
		_.assign(this._currentTable,this._currentRow);
		this._currentRow=null;
	},
	_end:function(){
		this._endRow();
		
		if(this._currentTable)this.tables.push(this._currentTable);
		this._currentTable=null;
	}
}

TSSet.prototype = {
	constructor:TSSet,
	tables:[],
	_currentTable:null,
	_currentRow:null,
	_handler:null,
	select:function(tableName){
		this._end();
		this._currentTable={tableName:tableName,rows:[]};
		
		return this;
	},
	put:function (...primary) {
		this._endRow();
		this._currentRow={
			type: 'PUT',
			condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
			primaryKey: [...primary],
			attributeColumns: [],
			returnContent: { returnType: TableStore.ReturnType.Primarykey }
		}
		return this;
	},
	update:function(...primary){
		this._endRow();
		this._currentRow={
			type: 'UPDATE',
			condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
			primaryKey: [...primary],
			attributeColumns: [],
			returnContent: { returnType: TableStore.ReturnType.Primarykey }
		}
		return this;
	},
	attr:function(...columns){
		var condition=_.reduce(columns,function (prev,item,index) {
			return _.concat(prev,_.map(item,function (value, key) {
				return _.pick(item,[key]);
			}));
		},[]);
		
		if(this._currentRow.type=='PUT'){
			this._currentRow.attributeColumns=condition;
		}else{
			this._currentRow.attributeColumns=[{"PUT":condition}];
		}
		return this;
	},
	exec:function(handler){
		this._handler=handler;
		
		this._end();
		let tb=this.tables;
		this.tables=[];
		client.batchWriteRow({tables:tb}, this._handler);
	},
	trace:function(){
		this._end();
		let tb=this.tables;
		this.tables=[];
		return {tables:tb};
	},
	_endRow:function(){
		if(this._currentRow){
			this._currentTable.rows.push(this._currentRow);
			this._currentRow=null;
		}
	},
	_end:function(){
		this._endRow();
		
		if(this._currentTable)this.tables.push(this._currentTable);
		this._currentTable=null;
	}
}

TSRange.prototype={
	constructor:TSRange,
	_tableName:'',
	_params:{},
	select:function(tableName){
		this._tableName=tableName;
		this._params={
			tableName: this._tableName,
			direction: TableStore.Direction.FORWARD,
			limit: 5000
		};
		return this;
	},
	range:function(...primary){
		this._params.inclusiveStartPrimaryKey=_.reduce(primary,function(prev,value,index){
			let obj={};
			obj[value]=TableStore.INF_MIN;
			prev.push(obj);
			return prev;
		},[]);
		this._params.exclusiveEndPrimaryKey=_.reduce(primary,function(prev,value,index){
			let obj={};
			obj[value]=TableStore.INF_MAX;
			prev.push(obj);
			return prev;
		},[]);
		
		return this;
	},
	equal(condition,operator="and"){
		if(_.isEmpty(condition))return this;
		let size=_.size(condition);
		if(size==1){
			condition=_.toPairs(condition);
			this._params.columnFilter=new TableStore.SingleColumnCondition(condition[0][0],condition[0][1], TableStore.ComparatorType.EQUAL);
			return this;
		}
		
		let lop;
		if(operator=="and")lop=TableStore.LogicalOperator.AND;
		else if(operator=="or")lop=TableStore.LogicalOperator.OR
		else lop=TableStore.LogicalOperator.NOT;
	
		var cond= new TableStore.CompositeCondition(lop);
		
		_.each(condition,function(value,index){
			cond.addSubCondition(new TableStore.SingleColumnCondition(index,value, TableStore.ComparatorType.EQUAL));
		})
		this._params.columnFilter = cond;
		return this;
	},
	trace(){
		return this._params;
	},
	exec:function(handler){
		client.getRange(this._params, handler);
	}
}

TSHelper.prototype={
	constructor:TSHelper,
	toBuffer:function(obj){
		return new Buffer(JSON.stringify(obj))
	},
	toNumber:function(num){
		return Long.fromNumber(num)
	},
	parse:function(data){
		let dt=[];
		
		_.each(data.tables,function(table){
			_.each(table,function(item){
				let attr={};
				_.each(item.attributes,function(column){
					if(typeof(column["columnValue"])=="object"){
						attr[column["columnName"]]=column["columnValue"].toString();
					}else{
						attr[column["columnName"]]=column["columnValue"];
					}
				})
				
				_.each(item.primaryKey,function (hash) {
					attr[hash.name]=hash.value;
				})
				
				dt.push(attr);
			})
		})
		return dt;
	},
	parseRange:function(data){
		let dt=[];
		_.each(data.rows,function (item, index) {
			let attr={};
			_.each(item.attributes,function(column){
				if(typeof(column["columnValue"])=="object"){
					attr[column["columnName"]]=column["columnValue"].toString();
				}else{
					attr[column["columnName"]]=column["columnValue"];
				}
			})
			
			_.each(item.primaryKey,function (hash) {
				attr[hash.name]=hash.value;
			})
			dt.push(attr);
		})
		let dat={list:dt};
		
		if(_.has(data,"next_start_primary_key")){
			dat.next_start_primary_key=_.get(data,"next_start_primary_key");
		}
		return dat;
	},
	response:function(json){
		var jsonData=JSON.stringify(json);
		var jsonResponse = {
			isBase64Encoded: true,
			statusCode: 200,
			headers: {
				"Content-type": "application/json"
			},
			// base64 encode body so it can be safely returned as JSON value
			body: new Buffer(jsonData).toString('base64')
		}
		return jsonResponse;
	}
}

exports.TSHelper=new TSHelper();
exports.TSGet=new TSGet();
exports.TSSet=new TSSet();
exports.TSRange=new TSRange();
exports.TSCreate=new TSCreate();
