package aws

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/datapipeline"
)

type pipelineObjectTestCase struct {
	Attrs    map[string]interface{}
	Expected *datapipeline.PipelineObject
}

func TestBuildCommonPipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":   "bar",
				"name": "boo",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
			},
		},
	}

	for i, testCase := range testCases {
		result := buildCommonPipelineObject(testCase.Attrs)

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}
}

func TestBuildDefaultPipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"schedule_type":          "cron",
				"failure_and_rerun_mode": "CASCADE",
				"pipeline_log_uri":       "s3://bucket-name/key-name-prefix/",
				"role":                   "DataPipelineDefaultRole",
				"resource_role":          "DataPipelineDefaultResourceRole",
				"schedule":               "myDefaultSchedule",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("Default"),
				Name: aws.String("Default"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("Default"),
					},
					{
						Key:         aws.String("scheduleType"),
						StringValue: aws.String("cron"),
					},
					{
						Key:         aws.String("failureAndRerunMode"),
						StringValue: aws.String("CASCADE"),
					},
					{
						Key:         aws.String("pipelineLogUri"),
						StringValue: aws.String("s3://bucket-name/key-name-prefix/"),
					},
					{
						Key:         aws.String("role"),
						StringValue: aws.String("DataPipelineDefaultRole"),
					},
					{
						Key:         aws.String("resourceRole"),
						StringValue: aws.String("DataPipelineDefaultResourceRole"),
					},
					{
						Key:      aws.String("schedule"),
						RefValue: aws.String("myDefaultSchedule"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildDefaultPipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}
}

func TestBuildEc2ResourcePipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":   "bar",
				"name": "boo",
				"associate_public_ip_address": true,
				"availability_zone":           "ap-northeast-1a",
				"image_id":                    "i-xxxxxxxxxxxxxx",
				"instance_type":               "t2.micro",
				"max_active_instances":        10,
				"maximum_retries":             5,
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("Ec2Resource"),
					},
					{
						Key:         aws.String("associatePublicIpAddress"),
						StringValue: aws.String("true"),
					},
					{
						Key:         aws.String("availabilityZone"),
						StringValue: aws.String("ap-northeast-1a"),
					},
					{
						Key:         aws.String("imageId"),
						StringValue: aws.String("i-xxxxxxxxxxxxxx"),
					},
					{
						Key:         aws.String("instanceType"),
						StringValue: aws.String("t2.micro"),
					},
					{
						Key:         aws.String("maxActiveInstances"),
						StringValue: aws.String("10"),
					},
					{
						Key:         aws.String("maximumRetries"),
						StringValue: aws.String("5"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildEc2ResourcePipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}

}

func TestBuildJdbcDatabasePipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":                  "bar",
				"name":                "boo",
				"connection_string":   "jdbc:mysql://localhost:3306/dbname",
				"database_name":       "dbname",
				"jdbc_driver_class":   "com.mysql.jdbc.Driver",
				"jdbc_driver_jar_uri": "s3://mysql-downloads/drivers/mysql-connector-java-5.1.40-bin.jar",
				"jdbc_properties": []string{
					"allowMultiQueries=true",
					"zeroDateTimeBehavior=convertToNull",
				},
				"username": "hogehoge",
				"password": "fugafuga",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("JdbcDatabase"),
					},
					{
						Key:         aws.String("connectionString"),
						StringValue: aws.String("jdbc:mysql://localhost:3306/dbname"),
					},
					{
						Key:         aws.String("databaseName"),
						StringValue: aws.String("dbname"),
					},
					{
						Key:         aws.String("jdbcDriverClass"),
						StringValue: aws.String("com.mysql.jdbc.Driver"),
					},
					{
						Key:         aws.String("jdbcDriverJarUri"),
						StringValue: aws.String("s3://mysql-downloads/drivers/mysql-connector-java-5.1.40-bin.jar"),
					},
					{
						Key:         aws.String("jdbcProperties"),
						StringValue: aws.String("allowMultiQueries=true"),
					},
					{
						Key:         aws.String("jdbcProperties"),
						StringValue: aws.String("zeroDateTimeBehavior=convertToNull"),
					},
					{
						Key:         aws.String("username"),
						StringValue: aws.String("hogehoge"),
					},
					{
						Key:         aws.String("*password"),
						StringValue: aws.String("fugafuga"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildJdbcDatabasePipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}
}

func TestBuildRdsDatabasePipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":                  "bar",
				"name":                "boo",
				"database_name":       "dbname",
				"jdbc_driver_jar_uri": "s3://mysql-downloads/drivers/mysql-connector-java-5.1.40-bin.jar",
				"jdbc_properties": []string{
					"allowMultiQueries=true",
					"zeroDateTimeBehavior=convertToNull",
				},
				"username":        "hogehoge",
				"password":        "fugafuga",
				"rds_instance_id": "my_db_instance_identifier",
				"region":          "ap-northeast-1",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("RdsDatabase"),
					},
					{
						Key:         aws.String("databaseName"),
						StringValue: aws.String("dbname"),
					},
					{
						Key:         aws.String("jdbcDriverJarUri"),
						StringValue: aws.String("s3://mysql-downloads/drivers/mysql-connector-java-5.1.40-bin.jar"),
					},
					{
						Key:         aws.String("jdbcProperties"),
						StringValue: aws.String("allowMultiQueries=true"),
					},
					{
						Key:         aws.String("jdbcProperties"),
						StringValue: aws.String("zeroDateTimeBehavior=convertToNull"),
					},
					{
						Key:         aws.String("username"),
						StringValue: aws.String("hogehoge"),
					},
					{
						Key:         aws.String("*password"),
						StringValue: aws.String("fugafuga"),
					},
					{
						Key:         aws.String("rdsInstanceId"),
						StringValue: aws.String("my_db_instance_identifier"),
					},
					{
						Key:         aws.String("region"),
						StringValue: aws.String("ap-northeast-1"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildRdsDatabasePipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}
}

func TestBuildS3DataNodePipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":             "bar",
				"name":           "boo",
				"compression":    "none",
				"directory_path": "hogehoge",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("S3DataNode"),
					},
					{
						Key:         aws.String("compression"),
						StringValue: aws.String("none"),
					},
					{
						Key:         aws.String("directory_path"),
						StringValue: aws.String("hogehoge"),
					},
				},
			},
		},
		{
			map[string]interface{}{
				"id":             "bar",
				"name":           "boo",
				"compression":    "gzip",
				"directory_path": "hogehoge",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("S3DataNode"),
					},
					{
						Key:         aws.String("compression"),
						StringValue: aws.String("gzip"),
					},
					{
						Key:         aws.String("directory_path"),
						StringValue: aws.String("hogehoge"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildS3DataNodePipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}
}

func TestBuildSQLDataNodePipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":               "bar",
				"name":             "boo",
				"database":         "myDataBaseName",
				"table":            "hogehoge",
				"create_table_sql": "CREATE TABLE #{table}",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("SqlDataNode"),
					},
					{
						Key:      aws.String("database"),
						RefValue: aws.String("myDataBaseName"),
					},
					{
						Key:         aws.String("table"),
						StringValue: aws.String("hogehoge"),
					},
					{
						Key:         aws.String("createTableSql"),
						StringValue: aws.String("CREATE TABLE #{table}"),
					},
				},
			},
		},
		{
			map[string]interface{}{
				"id":           "bar",
				"name":         "boo",
				"database":     "myDataBaseName",
				"table":        "hogehoge",
				"select_query": "select * from #{table} where eventTime >= '#{@scheduledStartTime.format('YYYY-MM-dd HH:mm:ss')}' and eventTime < '#{@scheduledEndTime.format('YYYY-MM-dd HH:mm:ss')}'",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("SqlDataNode"),
					},
					{
						Key:      aws.String("database"),
						RefValue: aws.String("myDataBaseName"),
					},
					{
						Key:         aws.String("table"),
						StringValue: aws.String("hogehoge"),
					},
					{
						Key:         aws.String("selectQuery"),
						StringValue: aws.String("select * from #{table} where eventTime >= '#{@scheduledStartTime.format('YYYY-MM-dd HH:mm:ss')}' and eventTime < '#{@scheduledEndTime.format('YYYY-MM-dd HH:mm:ss')}'"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildSQLDataNodePipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}
}

func TestBuildCopyActivityPipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":      "bar",
				"name":    "boo",
				"runs_on": "myEc2Resource",
				"input":   "mySQLInput",
				"output":  "myS3Output",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("CopyActivity"),
					},
					{
						Key:      aws.String("runsOn"),
						RefValue: aws.String("myEc2Resource"),
					},
					{
						Key:      aws.String("input"),
						RefValue: aws.String("mySQLInput"),
					},
					{
						Key:      aws.String("output"),
						RefValue: aws.String("myS3Output"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildCopyActivityPipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}

}

func TestBuildSnsAlarmPipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":        "bar",
				"name":      "boo",
				"topic_arn": "arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic",
				"subject":   "COPY SUCCESS: #{node.@scheduledStartTime}",
				"message":   "Files were copied from #{node.input} to #{node.output}.",
				"role":      "arn:aws:iam::account-id:role/role-name",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("SnsAlarm"),
					},
					{
						Key:         aws.String("message"),
						StringValue: aws.String("Files were copied from #{node.input} to #{node.output}."),
					},
					{
						Key:         aws.String("role"),
						StringValue: aws.String("arn:aws:iam::account-id:role/role-name"),
					},
					{
						Key:         aws.String("subject"),
						StringValue: aws.String("COPY SUCCESS: #{node.@scheduledStartTime}"),
					},
					{
						Key:         aws.String("topicArn"),
						StringValue: aws.String("arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildSnsAlarmPipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}

}

func TestBuildTerminatePipelineObject(t *testing.T) {
	testCases := []pipelineObjectTestCase{
		{
			map[string]interface{}{
				"id":   "bar",
				"name": "boo",
			},
			&datapipeline.PipelineObject{
				Id:   aws.String("bar"),
				Name: aws.String("boo"),
				Fields: []*datapipeline.Field{
					{
						Key:         aws.String("type"),
						StringValue: aws.String("Terminate"),
					},
				},
			},
		},
	}

	for i, testCase := range testCases {
		result, err := buildTerminatePipelineObject(testCase.Attrs)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf(
				"test case %d: got %#v, but want %#v",
				i, result, testCase.Expected,
			)
		}
	}

}
