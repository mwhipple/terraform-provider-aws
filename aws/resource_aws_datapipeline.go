package aws

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/datapipeline"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
)

var dataPipelineScheduleTypeList = []string{
	"cron",
	"ondemand",
	"timeseries",
}

var dataPipelineActionOnResourceFailureList = []string{
	"retryall",
	"retrynone",
}

var dataPipelineActionOnTaskFailureList = []string{
	"continue",
	"terminate",
}

var dataPipelineCompressionTypeList = []string{
	"none",
	"gzip",
}
var dataPipelineS3EncryptionTypeList = []string{
	"SERVER_SIDE_ENCRYPTION",
	"NONE",
}

var dataPipelineParameterObjectTypeList = []string{
	"String",
	"Integer",
	"Double",
	"AWS::S3::ObjectKey",
}

var dataPipelineFailureAndRerunModeList = []string{
	"cascade",
	"none",
}

func resourceAwsDataPipeline() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsDataPipelineCreate,
		Read:   resourceAwsDataPipelineRead,
		Schema: map[string]*schema.Schema{
			"activate": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},

			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"unique_id": {
				Type:     schema.TypeString,
				Required: true,
			},

			"pipeline_id": {
				Type:     schema.TypeString,
				Computed: true,
			},

			// Default
			"default": {
				Type:     schema.TypeMap,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"schedule_type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineScheduleTypeList, false),
						},

						"failure_and_rerun_mode": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineFailureAndRerunModeList, false),
							Default:      "none",
						},

						"pipeline_log_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"role": {
							Type:     schema.TypeString,
							Required: true,
						},

						"resource_role": {
							Type:     schema.TypeString,
							Required: true,
						},

						"schedule": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},

			// Activity
			"copy_activities": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},

			// Resources
			"ec2_resources": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"action_on_resource_failure": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineActionOnResourceFailureList, false),
						},

						"action_on_task_failure": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineActionOnTaskFailureList, false),
						},

						"associate_public_ip_address": {
							Type:     schema.TypeBool,
							Optional: true,
						},

						"availability_zone": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"image_id": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"init_timeout": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"instance_type": {
							Type:     schema.TypeString,
							Required: true,
						},

						"key_pair": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"late_after_timeout": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"max_active_instances": {
							Type:     schema.TypeInt,
							Optional: true,
						},

						"maximum_retries": {
							Type:     schema.TypeInt,
							Optional: true,
						},

						"on_fail": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_late_action": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_success": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"pipeline_log_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"region": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"schedule_type": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineScheduleTypeList, false),
						},
					},
				},
			},

			// Databases
			"jdbc_databases": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"connection_string": {
							Type:     schema.TypeString,
							Required: true,
						},

						"database_name": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"jdbc_driver_class": {
							Type:     schema.TypeString,
							Required: true,
						},

						"jdbc_driver_jar_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"jdbc_properties": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},

						"username": {
							Type:     schema.TypeString,
							Required: true,
						},

						"password": {
							Type:      schema.TypeString,
							Required:  true,
							Sensitive: true,
						},
					},
				},
			},

			"rds_databases": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"database_name": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"jdbc_driver_jar_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"jdbc_properties": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},

						"username": {
							Type:     schema.TypeString,
							Required: true,
						},

						"password": {
							Type:      schema.TypeString,
							Required:  true,
							Sensitive: true,
						},

						"rds_instance_id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"region": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},

			"s3_data_nodes": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"compression": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineCompressionTypeList, false),
						},

						"data_format": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"depends_on": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"directory_path": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"file_path"},
						},

						"failure_and_rerun_mode": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineFailureAndRerunModeList, false),
							Default:      "none",
						},

						"file_path": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"directory_path"},
						},

						"maximum_retries": {
							Type:     schema.TypeInt,
							Optional: true,
						},

						"on_fail": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_late_action": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_success": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"pipeline_log_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"s3_encryption_type": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice(dataPipelineS3EncryptionTypeList, false),
						},
					},
				},
			},

			"sql_data_nodes": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"schedule": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"database": {
							Type:     schema.TypeString,
							Required: true,
						},

						"table": {
							Type:     schema.TypeString,
							Required: true,
						},

						"create_table_sql": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"insert_query": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"select_query": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_fail": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_late_action": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"on_success": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"precondition": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"pipeline_log_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},

			// Data Formats
			"csv_data_formats": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"column": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},

						"escape_char": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},

			"tsv_data_formats": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"column": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},

						"column_separator": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"escape_char": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},

			// Actions
			"sns_alarms": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"message": {
							Type:     schema.TypeString,
							Required: true,
						},

						"role": {
							Type:     schema.TypeString,
							Required: true,
						},

						"subject": {
							Type:     schema.TypeString,
							Required: true,
						},

						"topic_arn": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},

			"terminates": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},

			"schedules": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},

						"name": {
							Type:     schema.TypeString,
							Required: true,
						},

						"period": {
							Type:     schema.TypeString,
							Required: true,
						},

						"start_at": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"start_date_time"},
						},

						"start_date_time": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"start_at"},
						},

						"end_date_time": {
							Type:     schema.TypeString,
							Optional: true,
						},

						"occurrences": {
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},

			"parameter_objects": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"description": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"type": {
							Type:         schema.TypeString,
							Optional:     true,
							Default:      "String",
							ValidateFunc: validation.StringInSlice(dataPipelineParameterObjectTypeList, false),
						},
						"optional": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"allowed_values": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"default": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"is_array": {
							Type:     schema.TypeBool,
							Optional: true,
						},
					},
				},
			},

			"parameter_values": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"string_value": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},

			"tags": tagsSchema(),
		},
	}
}

func resourceAwsDataPipelineCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).datapipelineconn

	uniqueID := d.Get("unique_id").(string)
	name := d.Get("name").(string)
	description := d.Get("description").(string)
	tags := tagsFromMapDataPipeline(d.Get("tags").(map[string]interface{}))

	input := datapipeline.CreatePipelineInput{
		Name:        aws.String(name),
		Description: aws.String(description),
		UniqueId:    aws.String(uniqueID),
		Tags:        tags,
	}

	resp, err := conn.CreatePipeline(&input)

	if err != nil {
		return err
	}

	log.Printf("[DEBUG] DataPipeline received: %s", resp)
	d.SetId(*resp.PipelineId)
	d.Set("unique_id", uniqueID)
	d.Set("tags", tagsToMapDataPipeline(tags))

	return resourceAwsDataPipelineUpdate(d, meta)
}

func resourceAwsDataPipelineUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).datapipelineconn
	pipelineID := d.Get("pipeline_id").(string)

	parameterObjects := d.Get("parameter_objects").([]interface{})
	parameterObjectConfigs := make([]*datapipeline.ParameterObject, 0, len(parameterObjects))
	for _, p := range parameterObjects {
		p := p.(map[string]interface{})
		po, err := buildParameterObject(p)
		if err != nil {
			return err
		}
		parameterObjectConfigs = append(parameterObjectConfigs, po)
	}

	parameterValues := d.Get("parameter_values").([]interface{})
	parameterValuesConfigs := make([]*datapipeline.ParameterValue, 0, len(parameterValues))
	for _, c := range parameterValues {
		pv := &datapipeline.ParameterValue{}
		c := c.(map[string]interface{})
		if val, ok := c["id"].(string); ok && val != "" {
			pv.SetId(val)
		}
		if val, ok := c["string_value"].(string); ok && val != "" {
			pv.SetStringValue(val)
		}
		parameterValuesConfigs = append(parameterValuesConfigs, pv)
	}

	pipelineObjectsConfigs := []*datapipeline.PipelineObject{}

	if v, ok := d.GetOk("default"); ok && v != nil {
		v := v.(map[string]interface{})
		po, err := buildCopyActivityPipelineObject(v)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	} else {
		log.Printf("[DEBUG] Not setting default pipeline object for %q", d.Id())
	}

	copyActivities := d.Get("copy_activity").([]interface{})
	for _, c := range copyActivities {
		c := c.(map[string]interface{})
		po, err := buildCopyActivityPipelineObject(c)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	csvDataFormats := d.Get("csv_data_formats").([]interface{})
	for _, c := range csvDataFormats {
		c := c.(map[string]interface{})
		po, err := buildCsvDataFormatPipelineObject(c)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	tsvDataFormats := d.Get("tsv_data_formats").([]interface{})
	for _, t := range tsvDataFormats {
		t := t.(map[string]interface{})
		po, err := buildTsvDataFormatPipelineObject(t)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	sqlDataNodes := d.Get("sql_data_nodes").([]interface{})
	for _, s := range sqlDataNodes {
		s := s.(map[string]interface{})
		po, err := buildSQLDataNodePipelineObject(s)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	ec2Resources := d.Get("ec2_resources").([]interface{})
	for _, e := range ec2Resources {
		e := e.(map[string]interface{})
		po, err := buildEc2ResourcePipelineObject(e)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	jdbcDatabases := d.Get("jdbc_databases").([]interface{})
	for _, j := range jdbcDatabases {
		j := j.(map[string]interface{})
		po, err := buildJdbcDatabasePipelineObject(j)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	rdsDatabases := d.Get("rds_databases").([]interface{})
	for _, r := range rdsDatabases {
		r := r.(map[string]interface{})
		po, err := buildRdsDatabasePipelineObject(r)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	snsAlarms := d.Get("sns_alarms").([]interface{})
	for _, s := range snsAlarms {
		s := s.(map[string]interface{})
		po, err := buildSnsAlarmPipelineObject(s)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	terminates := d.Get("terminates").([]interface{})
	for _, t := range terminates {
		t := t.(map[string]interface{})
		po, err := buildTerminatePipelineObject(t)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	schedules := d.Get("schedules").([]interface{})
	for _, s := range schedules {
		s := s.(map[string]interface{})
		po, err := buildSchedulePipelineObject(s)
		if err != nil {
			return err
		}
		pipelineObjectsConfigs = append(pipelineObjectsConfigs, po)
	}

	input := &datapipeline.PutPipelineDefinitionInput{
		PipelineId: aws.String(pipelineID),
	}
	if len(parameterObjectConfigs) > 0 {
		input.SetParameterObjects(parameterObjectConfigs)
	}

	if len(parameterValuesConfigs) > 0 {
		input.SetParameterValues(parameterValuesConfigs)
	}

	if len(pipelineObjectsConfigs) > 0 {
		input.SetPipelineObjects(pipelineObjectsConfigs)
	}

	err := input.Validate()
	if err != nil {
		return err
	}

	resp, err := conn.PutPipelineDefinition(input)

	if err != nil {
		return err
	}

	for _, validationWarn := range resp.ValidationWarnings {
		for _, warn := range validationWarn.Warnings {
			log.Printf("[WARN] %s:  %s", *validationWarn.Id, *warn)
		}
	}

	for _, validationError := range resp.ValidationErrors {
		for _, err := range validationError.Errors {
			log.Printf("[ERROR] %s:  %s", *validationError.Id, *err)
		}
	}

	if err := setTagsDataPipeline(conn, d); err != nil {
		return err
	}

	d.SetPartial("tags")
	d.Partial(false)

	return resourceAwsDataPipelineRead(d, meta)
}

func resourceAwsDataPipelineRead(d *schema.ResourceData, meta interface{}) error {

	v, err := resourceAwsDataPipelineRetrieve(d.Id(), meta.(*AWSClient).datapipelineconn)

	if err != nil {
		return err
	}
	if v == nil {
		d.SetId("")
		return nil
	}

	d.SetId(*v.PipelineId)
	d.Set("name", v.Name)
	d.Set("description", v.Description)
	d.Set("pipeline_id", v.PipelineId)
	d.Set("tags", tagsToMapDataPipeline(v.Tags))

	r, err := resourceAwsDataPipelineDefinitionRetrieve(d.Id(), meta.(*AWSClient).datapipelineconn)
	if err != nil {
		return err
	}
	// ParameterObjects
	if err := d.Set("parameterObjects", flattenParameterObjects(r.ParameterObjects)); err != nil {
		return fmt.Errorf("error reading DataPipeline \"%s\" parameter objects: %s", d.Id(), err.Error())
	}

	return nil
}

func resourceAwsDataPipelineDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).datapipelineconn

	log.Printf("[DEBUG] DataPipeline  destroy: %v", d.Id())

	opts := datapipeline.DeletePipelineInput{
		PipelineId: aws.String(d.Id()),
	}

	log.Printf("[DEBUG] DataPipeline destroy configuration: %v", opts)
	_, err := conn.DeletePipeline(&opts)
	if err != nil {
		if dataeerr, ok := err.(awserr.Error); ok && dataeerr.Code() == "PipelineNotFoundException" {
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error deleting Data Pipeline %s: %s", d.Id(), err.Error())
	}
	d.SetId("")

	return nil
}

// resourceAwsDataPipelineRetrieve fetches DataPipeline information from the AWS
// API. It returns an error if there is a communication problem or unexpected
// error with AWS. When the Data Pipeline is not found or deleted, it returns no error and a
// nil pointer.
func resourceAwsDataPipelineRetrieve(id string, conn *datapipeline.DataPipeline) (*datapipeline.PipelineDescription, error) {
	opts := datapipeline.DescribePipelinesInput{
		PipelineIds: []*string{aws.String(id)},
	}

	log.Printf("[DEBUG] Data Pipeline describe configuration: %#v", opts)

	resp, err := conn.DescribePipelines(&opts)
	if err != nil {
		datapipelineerr, ok := err.(awserr.Error)
		if ok && datapipelineerr.Code() == "PipelineNotFoundException" {
			return nil, nil
		} else if ok && datapipelineerr.Code() == "PipelineDeletedException" {
			return nil, nil
		}
		return nil, fmt.Errorf("Error retrieving Data Pipeline: %s", err.Error())
	}

	if len(resp.PipelineDescriptionList) != 1 ||
		*resp.PipelineDescriptionList[0].PipelineId != id {
		if err != nil {
			return nil, nil
		}
	}

	return resp.PipelineDescriptionList[0], nil
}

// resourceAwsDataPipelineDefinitionRetrieve fetches DataPipeline information from the AWS
// API. It returns an error if there is a communication problem or unexpected
// error with AWS. When the Data Pipeline is not found or deleted, it returns no error and a
// nil pointer.
func resourceAwsDataPipelineDefinitionRetrieve(id string, conn *datapipeline.DataPipeline) (*datapipeline.GetPipelineDefinitionOutput, error) {
	opts := datapipeline.GetPipelineDefinitionInput{
		PipelineId: aws.String(id),
	}
	log.Printf("[DEBUG] Data Pipeline describe configuration: %#v", opts)

	resp, err := conn.GetPipelineDefinition(&opts)
	if err != nil {
		datapipelineerr, ok := err.(awserr.Error)
		if ok && datapipelineerr.Code() == datapipeline.ErrCodePipelineNotFoundException {
			return nil, nil
		} else if ok && datapipelineerr.Code() == datapipeline.ErrCodePipelineDeletedException {
			return nil, nil
		}
		return nil, fmt.Errorf("Error retrieving Data Pipeline Definition: %s", err.Error())
	}

	return resp, nil
}

func flattenParameterObjectAttributes(attributes []*datapipeline.ParameterAttribute) []map[string]interface{} {
	parameterObjectAttributes := make([]map[string]interface{}, 0, len(attributes))
	for _, attribute := range attributes {
		var attr map[string]interface{}
		attr["key"] = *attribute.Key
		attr["stringValue"] = *attribute.StringValue
		parameterObjectAttributes = append(parameterObjectAttributes, attr)
	}

	return parameterObjectAttributes
}

func flattenParameterObjects(objects []*datapipeline.ParameterObject) []map[string]interface{} {
	parameterObjects := make([]map[string]interface{}, 0, len(objects))
	for _, object := range objects {
		var obj map[string]interface{}

		obj["id"] = *object.Id
		obj["attributes"] = flattenParameterObjectAttributes(object.Attributes)
		parameterObjects = append(parameterObjects, obj)
	}

	return parameterObjects
}
