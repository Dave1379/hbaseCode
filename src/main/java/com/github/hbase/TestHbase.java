package com.github.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestHbase {

	private Connection connection;

	@Before
	public void init() throws IOException {
		Configuration configuration = new Configuration();
		// 配置ZooKeeper信息
		configuration.set("hbase.zookeeper.quorum", "node1:2181");
		// 创建连接
		connection = ConnectionFactory.createConnection(configuration);
	}

	@Test
	public void testCreateTable() throws IOException {
		// 从连接中获得一个Admin对象
		Admin admin = connection.getAdmin();
		TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_user"));

		// 定义user_info的列族
		ColumnFamilyDescriptorBuilder userInfo = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user_info"));
		userInfo.setMaxVersions(3); // 设置版本信息
		tableDescriptorBuilder.setColumnFamily(userInfo.build());

		// 定义user_info的列族
		ColumnFamilyDescriptorBuilder loginInfo = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("login_info"));
		tableDescriptorBuilder.setColumnFamily(loginInfo.build());

		admin.createTable(tableDescriptorBuilder.build());
		System.out.println("创建表成功!");
	}

	@Test
	public void testPut() throws IOException {
		Table table = connection.getTable(TableName.valueOf("tb_user"));
		String rowKey = "1001";
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes("user_info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
		put.addColumn(Bytes.toBytes("user_info"), Bytes.toBytes("address"), Bytes.toBytes("上海"));
		put.addColumn(Bytes.toBytes("login_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
		put.addColumn(Bytes.toBytes("login_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));

		// 插入数据
		table.put(put);

		// 关闭连接
		table.close();

		System.out.println("插入数据成功！");
	}

	@Test
	public void testGet() throws IOException {
		Table table = connection.getTable(TableName.valueOf("tb_user"));
		String rowKey = "1001";
		Get get = new Get(Bytes.toBytes(rowKey));

		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		for (Cell cell : cells) {
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "==> "
					+ Bytes.toString(CellUtil.cloneFamily(cell)) + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
					+ ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
		}
		table.close();
	}

	@Test
	public void testScan() throws IOException {
		Table table = connection.getTable(TableName.valueOf("tb_user"));

		Scan scan = new Scan();
		scan.setLimit(1); // 只查询一条数据
		ResultScanner scanner = table.getScanner(scan);
		Result result = null;
		// 迭代数据
		while ((result = scanner.next()) != null) {
			// 打印数据 获取所有的单元格
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				// 打印rowkey,family,qualifier,value
				System.out.println(
						Bytes.toString(CellUtil.cloneRow(cell)) + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
								+ "{" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":"
								+ Bytes.toString(CellUtil.cloneValue(cell)) + "}");
			}
		}
		table.close();
	}

	@Test
	public void testFilter() throws IOException {
		Table table = connection.getTable(TableName.valueOf("tb_user"));

		Scan scan = new Scan();
		// 只查询name字段
		// QualifierFilter qualifierFilter = new
		// QualifierFilter(CompareOperator.EQUAL,
		// new BinaryComparator("name".getBytes()));
		// scan.setFilter(qualifierFilter);

		// 只查询user_info列族
		// FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL,
		// new BinaryComparator("user_info".getBytes()));
		// scan.setFilter(familyFilter);

		// 查询值等于张三的数据
		ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator("张三".getBytes()));
		scan.setFilter(valueFilter);

		ResultScanner scanner = table.getScanner(scan);
		Result result = null;
		// 迭代数据
		while ((result = scanner.next()) != null) {
			// 打印数据 获取所有的单元格
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				// 打印rowkey,family,qualifier,value
				System.out.println(
						Bytes.toString(CellUtil.cloneRow(cell)) + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
								+ "{" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":"
								+ Bytes.toString(CellUtil.cloneValue(cell)) + "}");
			}
		}
		table.close();
	}

}