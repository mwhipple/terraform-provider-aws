package aws

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/datapipeline"
)

func buildCommonPipelineObject(rawPipelineObject map[string]interface{}) *datapipeline.PipelineObject {
	pipelineObject := &datapipeline.PipelineObject{}
	if val, ok := rawPipelineObject["id"].(string); ok && val != "" {
		pipelineObject.SetId(val)
	}
	if val, ok := rawPipelineObject["name"].(string); ok && val != "" {
		pipelineObject.SetName(val)
	}

	return pipelineObject
}

func buildDefaultPipelineObject(rawDefault map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := &datapipeline.PipelineObject{}
	fieldList := []*datapipeline.Field{}

	pipelineObject.SetId("Default")
	pipelineObject.SetName("Default")

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("Default"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawDefault["schedule_type"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("scheduleType"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawDefault["failure_and_rerun_mode"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("failureAndRerunMode"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawDefault["pipeline_log_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("pipelineLogUri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawDefault["role"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("role"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawDefault["resource_role"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("resourceRole"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawDefault["schedule"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("schedule"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildEc2ResourcePipelineObject(rawEc2Resource map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawEc2Resource)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("Ec2Resource"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawEc2Resource["associate_public_ip_address"].(bool); ok && val {
		f := &datapipeline.Field{
			Key:         aws.String("associatePublicIpAddress"),
			StringValue: aws.String(strconv.FormatBool(val)),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["availability_zone"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("availabilityZone"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["http_proxy"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("httpProxy"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["image_id"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("imageId"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["instance_type"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("instanceType"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["key_pair"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("keyPair"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["max_active_instances"].(int); ok && val != 0 {
		f := &datapipeline.Field{
			Key:         aws.String("maxActiveInstances"),
			StringValue: aws.String(strconv.Itoa(val)),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["maximum_retries"].(int); ok && val != 0 {
		f := &datapipeline.Field{
			Key:         aws.String("maximumRetries"),
			StringValue: aws.String(strconv.Itoa(val)),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["on_success"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onSuccess"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["on_fail"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onFail"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["on_late_action"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onLateAction"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["pipeline_log_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("pipelineLogUri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["region"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("region"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawEc2Resource["schedule_type"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("scheduleType"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildJdbcDatabasePipelineObject(rawJdbcDatabase map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawJdbcDatabase)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("JdbcDatabase"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawJdbcDatabase["connection_string"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("connectionString"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawJdbcDatabase["database_name"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("databaseName"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawJdbcDatabase["jdbc_driver_class"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("jdbcDriverClass"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawJdbcDatabase["jdbc_driver_jar_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("jdbcDriverJarUri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawJdbcDatabase["jdbc_properties"].([]string); ok && len(val) != 0 {
		for _, v := range val {
			f := &datapipeline.Field{
				Key:         aws.String("jdbcProperties"),
				StringValue: aws.String(v),
			}
			fieldList = append(fieldList, f)
		}
	}

	if val, ok := rawJdbcDatabase["username"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("username"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawJdbcDatabase["password"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("*password"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildRdsDatabasePipelineObject(rawRdsDatabase map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawRdsDatabase)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("RdsDatabase"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawRdsDatabase["database_name"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("databaseName"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawRdsDatabase["jdbc_driver_jar_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("jdbcDriverJarUri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawRdsDatabase["jdbc_properties"].([]string); ok && len(val) != 0 {
		for _, v := range val {
			f := &datapipeline.Field{
				Key:         aws.String("jdbcProperties"),
				StringValue: aws.String(v),
			}
			fieldList = append(fieldList, f)
		}
	}

	if val, ok := rawRdsDatabase["username"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("username"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawRdsDatabase["password"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("*password"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawRdsDatabase["rds_instance_id"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("rdsInstanceId"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawRdsDatabase["region"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("region"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildCopyActivityPipelineObject(rawCopyActivity map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawCopyActivity)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("CopyActivity"),
	}
	fieldList = append(fieldList, typeField)
	if val, ok := rawCopyActivity["schedule"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("schedule"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["runs_on"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("runsOn"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["worker_group"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("workerGroup"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["depends_on"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("dependsOn"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["input"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("input"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["output"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("output"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["on_success"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onSuccess"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["on_fail"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onFail"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["on_late_action"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onLateAction"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["precondition"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("precondition"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawCopyActivity["pipeline_log_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("pipelineLogUri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildCsvDataFormatPipelineObject(rawCsvDataFormat map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawCsvDataFormat)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("CSV"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawCsvDataFormat["column"].([]string); ok && len(val) != 0 {

	}
	if val, ok := rawCsvDataFormat["escape_char"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("escapeChar"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildTsvDataFormatPipelineObject(rawTsvDataFormat map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawTsvDataFormat)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("TSV"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawTsvDataFormat["column"].([]string); ok && len(val) != 0 {

	}

	if val, ok := rawTsvDataFormat["escape_char"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("escapeChar"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawTsvDataFormat["column_separator"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("columnSeparator"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)

	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}

	return pipelineObject, nil
}

func buildS3DataNodePipelineObject(rawS3DataNode map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawS3DataNode)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("S3DataNode"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawS3DataNode["compression"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("compression"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["data_format"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("data_format"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["depends_on"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("depends_on"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["directory_path"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("directory_path"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["file_path"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("file_path"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["failure_and_rerun_mode"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("failure_and_rerun_mode"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["maximum_retries"].(int); ok && val != 0 {
		f := &datapipeline.Field{
			Key:         aws.String("maximum_retries"),
			StringValue: aws.String(strconv.Itoa(val)),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["on_fail"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("on_fail"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["on_success"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("on_success"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["pipeline_log_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("pipeline_log_uri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawS3DataNode["s3_encryption_type"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("s3_encryption_type"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)
	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}
	return pipelineObject, nil
}

func buildSQLDataNodePipelineObject(rawSQLDataNode map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawSQLDataNode)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("SqlDataNode"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawSQLDataNode["schedule"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("schedule"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}
	if val, ok := rawSQLDataNode["database"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("database"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}
	if val, ok := rawSQLDataNode["table"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("table"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}
	if val, ok := rawSQLDataNode["create_table_sql"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("createTableSql"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}
	if val, ok := rawSQLDataNode["insert_query"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("insertQuery"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}
	if val, ok := rawSQLDataNode["select_query"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("selectQuery"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSQLDataNode["on_success"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onSuccess"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSQLDataNode["on_fail"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onFail"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSQLDataNode["on_late_action"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("onLateAction"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSQLDataNode["precondition"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:      aws.String("precondition"),
			RefValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSQLDataNode["pipeline_log_uri"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("pipelineLogUri"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)
	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}
	return pipelineObject, nil
}

func buildSnsAlarmPipelineObject(rawSnsAlarm map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawSnsAlarm)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("SnsAlarm"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawSnsAlarm["message"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("message"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSnsAlarm["role"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("role"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSnsAlarm["subject"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("subject"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSnsAlarm["topic_arn"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("topicArn"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)
	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}
	return pipelineObject, nil
}

func buildTerminatePipelineObject(rawTerminate map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawTerminate)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("Terminate"),
	}
	fieldList = append(fieldList, typeField)

	pipelineObject.SetFields(fieldList)
	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}
	return pipelineObject, nil
}

func buildSchedulePipelineObject(rawSchedule map[string]interface{}) (*datapipeline.PipelineObject, error) {
	pipelineObject := buildCommonPipelineObject(rawSchedule)
	fieldList := []*datapipeline.Field{}

	typeField := &datapipeline.Field{
		Key:         aws.String("type"),
		StringValue: aws.String("Schedule"),
	}
	fieldList = append(fieldList, typeField)

	if val, ok := rawSchedule["period"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("period"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSchedule["start_at"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("startAt"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSchedule["start_date_time"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("startDateTime"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSchedule["end_date_time"].(string); ok && val != "" {
		f := &datapipeline.Field{
			Key:         aws.String("endDateTime"),
			StringValue: aws.String(val),
		}
		fieldList = append(fieldList, f)
	}

	if val, ok := rawSchedule["occurrences"].(int); ok && val != 0 {
		f := &datapipeline.Field{
			Key:         aws.String("occurrences"),
			StringValue: aws.String(strconv.Itoa(val)),
		}
		fieldList = append(fieldList, f)
	}

	pipelineObject.SetFields(fieldList)
	err := pipelineObject.Validate()
	if err != nil {
		return nil, err
	}
	return pipelineObject, nil
}

func buildParameterObject(rawParameterObject map[string]interface{}) (*datapipeline.ParameterObject, error) {
	parameterObject := &datapipeline.ParameterObject{}
	parameterAttributeList := []*datapipeline.ParameterAttribute{}

	if val, ok := rawParameterObject["id"].(string); ok && val != "" {
		parameterObject.SetId(val)
	}

	if val, ok := rawParameterObject["description"].(string); ok && val != "" {
		p := &datapipeline.ParameterAttribute{
			Key:         aws.String("description"),
			StringValue: aws.String(val),
		}
		parameterAttributeList = append(parameterAttributeList, p)
	}

	if val, ok := rawParameterObject["type"].(string); ok && val != "" {
		p := &datapipeline.ParameterAttribute{
			Key:         aws.String("type"),
			StringValue: aws.String(val),
		}
		parameterAttributeList = append(parameterAttributeList, p)
	}

	if val, ok := rawParameterObject["optional"].(bool); ok {
		p := &datapipeline.ParameterAttribute{
			Key:         aws.String("optional"),
			StringValue: aws.String(strconv.FormatBool(val)),
		}
		parameterAttributeList = append(parameterAttributeList, p)
	}

	if val, ok := rawParameterObject["allowed_values"].([]string); ok && len(val) != 0 {
		for _, v := range val {
			p := &datapipeline.ParameterAttribute{
				Key:         aws.String("allowedValues"),
				StringValue: aws.String(v),
			}
			parameterAttributeList = append(parameterAttributeList, p)
		}
	}

	if val, ok := rawParameterObject["default"].(string); ok && val != "" {
		p := &datapipeline.ParameterAttribute{
			Key:         aws.String("default"),
			StringValue: aws.String(val),
		}
		parameterAttributeList = append(parameterAttributeList, p)
	}

	if val, ok := rawParameterObject["is_array"].(bool); ok {
		p := &datapipeline.ParameterAttribute{
			Key:         aws.String("isArray"),
			StringValue: aws.String(strconv.FormatBool(val)),
		}
		parameterAttributeList = append(parameterAttributeList, p)
	}

	parameterObject.SetAttributes(parameterAttributeList)
	err := parameterObject.Validate()
	if err != nil {
		return nil, err
	}

	return parameterObject, nil
}
