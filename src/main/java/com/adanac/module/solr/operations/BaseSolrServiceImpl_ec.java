package com.adanac.module.solr.operations;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;

/**
 * 创建时间：20160728 功能：抽取solr数据写到文件
 * 
 * @author 圣斗士宙斯
 *
 */
public abstract class BaseSolrServiceImpl_ec {
	protected static SolrClient client;

	/**
	 * 初始化solr连接
	 */
	private static void init() {
		try {
			client = new CloudSolrClient("zk_host1:port,zk_host2:port");// zookeeper的ip和端口
			((CloudSolrClient) client).setDefaultCollection("xxx");
			((CloudSolrClient) client).connect();
		} catch (Exception e) {
			client = new HttpSolrClient("http://solr_ip:port/solr/xxx");// solr库的链接
		}
	}

	/**
	 * 查询方法（可以不做排序过滤等）
	 */
	public static QueryResponse query(String query, String fields, int pageNumber, int size,
			List<SolrQuery.SortClause> fieldSorts, String... conditions) throws Exception {
		SolrQuery solrQuery = new SolrQuery();
		// 以下都是些控制条件，不用也可以删掉
		if (StringUtils.isBlank(query)) {
			solrQuery.setQuery("*:*");
			// solrQuery.addFacetQuery("*:*");无条件全表扫
		} else {
			solrQuery.setQuery(query);
		}
		size = size <= 0 ? 10 : size;
		pageNumber = pageNumber < 1 ? 0 : (pageNumber - 1) * size;
		solrQuery.setStart(pageNumber);
		solrQuery.setRows(size);
		if (StringUtils.isNotBlank(fields)) {
			solrQuery.setFields(fields);
		}
		if (fieldSorts != null) {
			for (SolrQuery.SortClause fieldSort : fieldSorts) {
				solrQuery.addSort(fieldSort);
			}
		}
		if (conditions != null) {
			if (conditions.length >= 1 && StringUtils.isNotBlank(conditions[0])) {
				solrQuery.setFilterQueries(conditions[0].split(",|，"));
			}
			if (conditions.length >= 2 && StringUtils.isNotBlank(conditions[1])) {
				String[] split = conditions[1].split(",|，");
				solrQuery.setParam("group", true);
				solrQuery.setParam("group.ngroups", true);
				solrQuery.setParam("group.field", split);
				solrQuery.setParam(GroupParams.GROUP_SORT, "pub_time desc");
			}
			if (conditions.length >= 3 && StringUtils.isNotBlank(conditions[2])) {
				String[] split = conditions[2].split(";|；");
				solrQuery.set(FacetParams.FACET, true);
				if (split.length > 1) {
					solrQuery.set("facet.date", split[0]);
					solrQuery.set("facet.date.start", split[1]);
					solrQuery.set("facet.date.end", split[2]);
					solrQuery.set("facet.date.gap", split[3]);
				} else {
					solrQuery.set("facet.field", split[0]);
				}
			}
			if (conditions.length >= 5 && StringUtils.isNotBlank(conditions[4])) {
				solrQuery.setHighlight(true);
				solrQuery.setParam("hl.fl", conditions[4]);
				solrQuery.setParam("hl.fragsize", "160");
				solrQuery.setParam("hl.snippets", "2");
				solrQuery.setParam("hl.mergeContiguous", "true");
				solrQuery.setHighlightSimplePre("<font class='word-highlight'>");
				solrQuery.setHighlightSimplePost("</font>");
			}
			if (conditions.length >= 6 && StringUtils.isNotBlank(conditions[5])) {
				solrQuery.setParam("hl.q", conditions[5]);
			}
		}
		if (client instanceof CloudSolrClient) {
			if (conditions.length >= 4 && StringUtils.isNotBlank(conditions[3])) {
				solrQuery.add("shards", conditions[3]);
			}
		}
		QueryResponse queryResponse = client.query(solrQuery, SolrRequest.METHOD.POST);
		return queryResponse;
	}

	public static void main(String[] args) {
		// 调用初始化连接方法
		init();
		// 接收传入的参数，这里只是对时间做限制，一天内24小时
		String whereColumn = args[0];// crawltime
		String time1 = args[1];// 2016-01-17
		String time2 = time1;
		// 由于按天分页抽取solr库的数据会非常慢，分为24此抽取，分页数按照当前小时的总数量/一页显示的数量
		try {
			QueryResponse response = null;
			FileWriter fw = null;// BufferedWriter FileWriter
			StringBuilder sb = new StringBuilder();// StringBuffer
			for (int i = 0; i <= 23; i++) {
				// String stringHour ;
				if (i <= 9) {
					// stringHour = "0"+(i+=1);
					time1 = time1 + "T0" + i + ":00:00Z";
					if (i + 1 == 10) {
						time2 = time2 + "T" + (i + 1) + ":00:00Z";
					} else {
						time2 = time2 + "T0" + (i + 1) + ":00:00Z";
					}
				} else {
					// stringHour = (i+=1)+"";
					time1 = time1 + "T" + i + ":00:00Z";
					time2 = time2 + "T" + (i + 1) + ":00:00Z";
				}
				response = query(whereColumn + ":[" + time1 + " TO " + time2 + "] && title:*手机*"
						+ " || ( weburl:*mobile.zol.com.cn* || weburl:*mo*)", null, 0, 5000, null);
				int numFound = (int) response.getResults().getNumFound();
				int fors = (int) Math.ceil((numFound / 5000));
				String string = time1.substring(0, 10).replace("-", "");
				System.out.println("第 " + time1 + "小时到第" + time2 + "小时 统计条数: " + numFound);// 统计条数
				System.out.println("第 " + time1 + "小时到第" + time2 + "小时 分页总数: " + (int) Math.ceil((numFound / 5000)));// 分页总数
				File file_Dir = new File("C:\\Users\\zhuqitian\\Desktop\\solrDate_ec\\" + string + "\\");
				if (!file_Dir.exists()) {
					file_Dir.mkdirs();
				}
				File file = new File("C:\\Users\\zhuqitian\\Desktop\\solrDate_ec\\" + string + "\\solrDate_ec_" + string
						+ "_" + i + ".txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				fw = new FileWriter(file, true);// FileOutputStream
				for (int k = 0; k <= fors; k++) {
					SolrDocumentList docs1 = response.getResults();
					response = query(whereColumn + ":[" + time1 + " TO " + time2 + "]", null, k, 5000, null);
					for (SolrDocument sd : docs1) {
						sb.append(sd.getFieldValue("doc_id") + "!@#");
						sb.append(sd.getFieldValue("dedup_id") + "!@#");
						sb.append(sd.getFieldValue("url") + "!@#");
						sb.append(sd.getFieldValue("author") + "!@#");
						sb.append(sd.getFieldValue("weburl") + "!@#");
						sb.append(sd.getFieldValue("media") + "!@#");
						sb.append(sd.getFieldValue("region") + "!@#");
						sb.append(sd.getFieldValue("source") + "!@#");
						sb.append(sd.getFieldValue("create_time") + "!@#");
						sb.append(sd.getFieldValue("summary") + "!@#");
						sb.append(sd.getFieldValue("keywords") + "!@#");
						sb.append(sd.getFieldValue("simple_keywords") + "!@#");
						sb.append(sd.getFieldValue("sentiment") + "!@#");
						sb.append(sd.getFieldValue("sentiment_val") + "!@#");
						sb.append(sd.getFieldValue("comment_count") + "!@#");
						sb.append(sd.getFieldValue("click_count") + "!@#");
						sb.append(sd.getFieldValue("crawltime") + "!@#");
						sb.append(sd.getFieldValue("title") + "!@#");
						sb.append(sd.getFieldValue("pub_time") + "!@#");
						sb.append("\n");
						// bytes = sb.toString().getBytes();
					}
					fw.write(sb.toString());
					System.out.println("第 " + i + " 小时到第 " + (i + 1) + "小时： 第 " + k + " 页写入完毕！文件名为 " + file.getName());
					sb.delete(0, sb.length());
				}
				time1 = args[1];
				time2 = time1;
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}