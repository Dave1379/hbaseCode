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
		// ����ZooKeeper��Ϣ
		configuration.set("hbase.zookeeper.quorum", "node1:2181");
		// ��������
		connection = ConnectionFactory.createConnection(configuration);
	}

	@Test
	public void testCreateTable() throws IOException {
		// �������л��һ��Admin����
		Admin admin = connection.getAdmin();
		TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_user"));

		// ����user_info������
		ColumnFamilyDescriptorBuilder userInfo = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user_info"));
		userInfo.setMaxVersions(3); // ���ð汾��Ϣ
		tableDescriptorBuilder.setColumnFamily(userInfo.build());

		// ����user_info������
		ColumnFamilyDescriptorBuilder loginInfo = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("login_info"));
		tableDescriptorBuilder.setColumnFamily(loginInfo.build());

		admin.createTable(tableDescriptorBuilder.build());
		System.out.println("������ɹ�!");
	}

	@Test
	public void testPut() throws IOException {
		Table table = connection.getTable(TableName.valueOf("tb_user"));
		String rowKey = "1001";
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes("user_info"), Bytes.toBytes("name"), Bytes.toBytes("����"));
		put.addColumn(Bytes.toBytes("user_info"), Bytes.toBytes("address"), Bytes.toBytes("�Ϻ�"));
		put.addColumn(Bytes.toBytes("login_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
		put.addColumn(Bytes.toBytes("login_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));

		// ��������
		table.put(put);

		// �ر�����
		table.close();

		System.out.println("�������ݳɹ���");
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
		scan.setLimit(1); // ֻ��ѯһ������
		ResultScanner scanner = table.getScanner(scan);
		Result result = null;
		// ��������
		while ((result = scanner.next()) != null) {
			// ��ӡ���� ��ȡ���еĵ�Ԫ��
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				// ��ӡrowkey,family,qualifier,value
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
		// ֻ��ѯname�ֶ�
		// QualifierFilter qualifierFilter = new
		// QualifierFilter(CompareOperator.EQUAL,
		// new BinaryComparator("name".getBytes()));
		// scan.setFilter(qualifierFilter);

		// ֻ��ѯuser_info����
		// FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL,
		// new BinaryComparator("user_info".getBytes()));
		// scan.setFilter(familyFilter);

		// ��ѯֵ��������������
		ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator("����".getBytes()));
		scan.setFilter(valueFilter);

		ResultScanner scanner = table.getScanner(scan);
		Result result = null;
		// ��������
		while ((result = scanner.next()) != null) {
			// ��ӡ���� ��ȡ���еĵ�Ԫ��
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				// ��ӡrowkey,family,qualifier,value
				System.out.println(
						Bytes.toString(CellUtil.cloneRow(cell)) + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
								+ "{" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":"
								+ Bytes.toString(CellUtil.cloneValue(cell)) + "}");
			}
		}
		table.close();
	}

}