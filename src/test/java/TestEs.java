import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

public class TestEs {

	private TransportClient client;
	
	public static TransportClient getClient() throws UnknownHostException{
		
		Settings settings = Settings.builder()
				//.put("cluster.name", "tl-application")//集群名称
				.put("cluster.name", "elasticsearch")//集群名称
				.put("client.transport.sniff", true)//开启集群的嗅探功能，只需要指定集群中一个节点信息即可获取到集群中的所有节点信息
				.build();
		
		//获取TransportClient
		TransportClient client =  new PreBuiltTransportClient(settings);
		//需要使用9300端口
		TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName("192.168.43.80"), 9300);
		//TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300);
		//TransportAddress transportAddress1 = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.164"), 9300);
		//TransportAddress transportAddress2 = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.168"), 9300);
		
		//添加节点信息，最少指定集群内的某一个节点即可操作这个es集群
		client.addTransportAddress(transportAddress);
		//client.addTransportAddress(transportAddress1);
		//client.addTransportAddress(transportAddress2);
		return client;
	}
	
	public void test0() throws Exception {
		//获取TransportClient
		client = new PreBuiltTransportClient(Settings.EMPTY);

		//需要使用9300端口
		//TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.153"), 9300);
		TransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.165"),9300);
		//添加节点信息，最少指定集群内的某一个节点即可操作这个es集群
		client.addTransportAddress(inetSocketTransportAddress);
		client.close();
	}
	
	/**
	 * 用java代码测试的时候这样写是没有问题的，比较简单
	 * @throws Exception
	 */

	
	/**
	 * 可以这样写，防止代码中指定的链接失效，。
	 * 但是写起来比较麻烦
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		//获取TransportClient
		TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
		//需要使用9300端口
		TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.100"), 9300);
		//TransportAddress transportAddress1 = new TransportAddress(InetAddress.getByName("192.168.1.101"), 9300);
		//TransportAddress transportAddress2 = new TransportAddress(InetAddress.getByName("192.168.1.102"), 9300);
		//添加节点信息，最少指定集群内的某一个节点即可操作这个es集群
		client.addTransportAddresses(transportAddress);
		System.out.println(client.toString());
	}
	
	/**
	 * 实际成产环境下面，建议这样用
	 * @throws Exception
	 */
	@Test
	public void test3() throws Exception {
		
		//获取client链接到的节点信息
		List<DiscoveryNode> connectedNodes = getClient().connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println(discoveryNode.getHostName());
		}
	}
	
	String index = "whdata-rsyslog-2017.08.08";
	String type = "rsyslog";
	/**
	 * index-1 json
	 * @throws Exception
	 */
	@Test
	public void test4() throws Exception {
		//String jsonStr = "{\"name\":\"jack\",\"age\":19}";
		TransportClient client = getClient();
		 BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		 try {
             FileReader reader = new FileReader("E://test//es_timestamp.txt");
           	 BufferedReader br = new BufferedReader(reader);
           	 String messageStr = null;
             while((messageStr = br.readLine()) != null) {
             	Map<String,String> map = new HashMap<>();
             	map.put("key","value");
            	IndexRequestBuilder indexRequestBuilder = client.prepareIndex("test", "timestamp").setSource(map);
     			bulkRequestBuilder.add(indexRequestBuilder);
     			if(bulkRequestBuilder.numberOfActions()>1){
                    BulkResponse bulkResponse = bulkRequestBuilder.get();
                    System.out.println(bulkResponse.hasFailures());
          			 break;
        		}
             }
             
             
            br.close();
            reader.close();
              
            } catch (Exception e) {  
                e.printStackTrace();  
            } 
		
	}
	
	/**
	 * index-2 hashmap
	 * @throws Exception
	 */
	@Test
	public void test5() throws Exception {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("name", "tom");
		hashMap.put("age", 15);
		IndexResponse indexResponse = client.prepareIndex(index, type, "2").setSource(hashMap).get();
		System.out.println(indexResponse.getVersion());
	}
	
	/**
	 * index-3 bean
	 * @throws Exception
	 */
	@Test
	public void test6() throws Exception {
		/*Person person = new Person();
		person.setName("mack");
		person.setAge(20);
		ObjectMapper objectMapper = new ObjectMapper();
		String writeValueAsString = objectMapper.writeValueAsString(person);
		IndexResponse indexResponse = client.prepareIndex(index, type, "3").setSource(writeValueAsString).get();
		System.out.println(indexResponse.getVersion());*/
	}
	
	/**
	 * index -4 es helper
	 * @throws Exception
	 */
	@Test
	public void test7() throws Exception {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
			.field("name", "jessic")
			.field("age", 28)
			.endObject();
		
		IndexResponse indexResponse = client.prepareIndex(index, type, "4").setSource(builder).get();
		System.out.println(indexResponse.getVersion());
	}
	
	/**
	 * get 查询
	 * @throws Exception
	 */
	@Test
	public void test8() throws Exception {
		//GetResponse getResponse = client.prepareGet(index, type, "4").get();
		GetResponse getFields = getClient().prepareGet("test", "test", "1").get();
		System.out.println(getFields.toString());
	}

    @Test
    public void searchAll()throws Exception{
        SearchRequestBuilder srb=getClient().prepareSearch("*").setTypes("timestamp");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
			System.out.println(hit.getIndex());
            System.out.println(hit.getSourceAsString());
        }
    }
	
	/**
	 * 局部更新
	 * @throws Exception
	 */
	@Test
	public void test9() throws Exception {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("age", 29).endObject();
		UpdateResponse updateResponse = client.prepareUpdate(index, type, "4").setDoc(builder).get();
		System.out.println(updateResponse.getVersion());
	}
	
	/**
	 * 删除
	 * @throws Exception
	 */
	@Test
	public void test10() throws Exception {
		client.prepareDelete(index, type, "4").get();
	}
	
	/**
	 * count 取总数  类似于sql中的 select count(1) from table;
	 */
/*	@Test
	public void test11() throws Exception {
		long count = client.prepareCount(index).setTypes(type).get().getCount();
		System.out.println(count);
	}*/
	
	/**
	 * bulk 批量操作 适合初始化数据的时候使用，提高效率
	 * @throws Exception
	 */
	@Test
	public void test12() throws Exception {
		BulkRequestBuilder prepareBulk = client.prepareBulk();
		
		//for循环执行----
		//index请求
		IndexRequest indexRequest = new IndexRequest(index, type, "10");
		indexRequest.source("{\"name\":\"zhangsan\",\"age\":17}");
		//delete请求
		DeleteRequest deleteRequest = new DeleteRequest(index, type, "1");
		
		
		prepareBulk.add(indexRequest );
		prepareBulk.add(deleteRequest);
		
		//执行 bulk
		BulkResponse bulkResponse = prepareBulk.get();
		if(bulkResponse.hasFailures()){
			//有执行失败的
			BulkItemResponse[] items = bulkResponse.getItems();
			for (BulkItemResponse bulkItemResponse : items) {
				//获取失败信息
				System.out.println(bulkItemResponse.getFailureMessage());
			}
		}else{
			System.out.println("全部执行成功！");
		}
		
	}
	
	/**
	 * 判断type是否存在
	 * @throws UnknownHostException
	 */
	@Test
	public void isExistsType() throws UnknownHostException{
		Settings settings = Settings.builder()
				.put("cluster.name", "iLogo")//集群名称
				.put("client.transport.sniff", true)//开启集群的嗅探功能，只需要指定集群中一个节点信息即可获取到集群中的所有节点信息
				.build();
		
		//获取TransportClient
		TransportClient client =  new PreBuiltTransportClient(settings);
		//需要使用9300端口
		TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.165"), 9300);
		//添加节点信息，最少指定集群内的某一个节点即可操作这个es集群
		client.addTransportAddress(transportAddress);
		String indexName="twitter";
		String indexType="tweet";
	    TypesExistsResponse  response = 
	            client.admin().indices()
	            .typesExists(new TypesExistsRequest(new String[]{indexName}, indexType)
	            ).actionGet();
	    System.out.println(response.isExists());
	}
	
	/**
	 * 判断index是否存在
	 * @throws UnknownHostException
	 */
	@Test
	public void isExistsIndex() throws UnknownHostException{
		
		String indexName="twitter";
	    IndicesExistsResponse  response = 
	    		getClient().admin().indices().exists( 
	                        new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();
	    System.out.println(response.isExists());
	}
	
	/**
	 * 创建index
	 * @throws UnknownHostException 
	 */
	@Test
	public void createIndex() throws UnknownHostException{
		CreateIndexRequest request=new CreateIndexRequest("cms");
		try {
			getClient().admin().indices().create(request).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 删除index
	 * @throws UnknownHostException
	 */
	@Test
	public void deleteIndex() throws UnknownHostException{
		DeleteIndexRequest request=new DeleteIndexRequest("test");
		try {
			getClient().admin().indices().delete(request).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建mapping
	 */
	 public XContentBuilder getMapping(){
		
		XContentBuilder mapping = null;
		try {
			mapping = XContentFactory.jsonBuilder()
					.startObject()
					.startObject("_ttl")
					.field("enabled",true)
					.field("default","1m")
					.endObject()
					.startObject("properties")
					.startObject("info")
					.field("type", "text")
					.field("analyzer", "ik_max_word")
					.endObject()
					.startObject("@timestamp")
					.field("type", "date")
					.field("format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
					.endObject()
					.startObject("host")
					.field("type", "text")
					.endObject()
					//.startObject("title")
					//.field("type", "text")
					// 指定index analyzer 为 ik
					//.field("analyzer", "ik")
					// 指定search_analyzer 为ik_syno
					//.field("searchAnalyzer", "ik_syno")
					//.endObject()
					//.startObject("description").field("type", "text")
					//.field("index", "not_analyzed").endObject()
					//.startObject("url").field("type", "text")
					//.field("index", "not_analyzed").endObject()
					//.startObject("type").field("type", "integer").endObject()
					.endObject().endObject();
			/*mapping = XContentFactory.jsonBuilder()
					.startObject()
					.startObject("properties")
					.startObject("serverity")
					.field("type", "integer")
					.endObject()
					.startObject("program")
					.field("type", "keyword")
					.endObject()
					.startObject("message")
					.field("type", "text")
					.endObject()
					.startObject("priority")
					.field("type", "integer")
					.endObject()
					.startObject("logsource")
					.field("type", "keyword")
					.endObject()
					.startObject("logtype")
					.field("type", "keyword")
					.endObject()
					.startObject("@timestamp")
					.field("type", "date")
					.field("format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
					.endObject()
					.startObject("@version")
					.field("type", "keyword")
					.endObject()
					.startObject("host")
					.field("type", "ip")
					.endObject()
					.startObject("facility")
					.field("type", "integer")
					.endObject()
					.startObject("severity_label")
					.field("type", "keyword")
					.endObject()
					.startObject("timestamp")
					.field("type", "date")
					.field("format", "epoch_millis")
					.endObject()
					.startObject("facility_label")
					.field("type", "keyword")
					.endObject()
					.endObject().endObject();*/
		} catch (IOException e) {
			e.printStackTrace();
		}
		return mapping;
	    }  
	 
	 @Test
	 public void createMapping() throws UnknownHostException{  
	        //先创建索引  
	       /* CreateIndexRequest request = new CreateIndexRequest("XXX");  
	        getClient().admin().indices().create(request);  */
	        //创建mapping
	        PutMappingRequest mapping = Requests.putMappingRequest("cms").type("trs").source(getMapping());
	        PutMappingResponse actionGet = getClient().admin().indices().putMapping(mapping).actionGet();
	        System.out.println(actionGet.isAcknowledged());
	          
	    }
	 
	 @Test
	 public void indexDataTest() throws UnknownHostException{
		 IndicesStatsResponse resp = getClient().admin().indices().prepareStats().setIndices("*-2017.08.08").execute().actionGet();
		 Map<String, IndexStats> indices = resp.getIndices();
		  for (Map.Entry<String, IndexStats> entry : indices.entrySet()) {
			  long sizeInBytes = entry.getValue().getPrimaries().getStore().getSizeInBytes();
			  System.out.println(sizeInBytes);
			  }
		  System.out.println(resp.getPrimaries().getStore().getSizeInBytes());
	 }
	 
	 @Test
	 public void deleteTemplate() throws UnknownHostException{
		DeleteIndexTemplateResponse response = getClient().admin().indices().prepareDeleteTemplate("ilogo*").execute().actionGet();  
		System.out.println(response.isAcknowledged());
	 }
	 
	 public static void main(String[] args) throws UnknownHostException {
		 System.out.println("Sendge".contains("Send"));
	}
}
