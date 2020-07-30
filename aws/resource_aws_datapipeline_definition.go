package aws

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/datapipeline"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	//	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
)

// This resource currently follows a pattern of defining different
// types of objects within the main resource. An alternative approach
// would be to allow each object type to be a separate local only resource
// which exposes a serialized attribute that could be pulled in to the
// definition object list. The nested approach provides a lot of extra
// noise in the structure and feels less extensible, whereas the split out
// approach may invite bloat in state files. More significantly the
// split out approach seems likely to pollute this provider with a lot
// of resources for a seemingly relatively unpopular AWS offering, so
// the nested approach will be stuck with for now.

// TODO: Support user defined properties
// TODO: Capture all schemata
// TODO: Call validate pipeline before requesting
// TODO: Add validation to each schema
// TODO: Default name argument to id

//
// TOP LEVEL
//

// resourceAwsDataPipelineDefinition defines the resource as a whole.
func resourceAwsDataPipelineDefinition() *schema.Resource {
	return &schema.Resource{
		Create:   resourceAwsDataPipelineDefinitionPut,
		Read:     resourceAwsDataPipelineDefinitionRead,
		Update:   resourceAwsDataPipelineDefinitionPut,
		Delete:   schema.Noop,
		Importer: &schema.ResourceImporter{
			State: resourceAwsDataPipelineDefinitionImport,
		},

		Schema:   pipelineDefinitionSchema(),
	}
}

//
// SCHEMA
//

//
// Define the exposed schemata.
//

// pipelineDefinitionSchma defines the top level schema.
// This will largely act as a mapping to the specialized object functions.
func pipelineDefinitionSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"pipeline_id": {
			Type:        schema.TypeString,
			Description: "Identify the (existing) pipeline which will be defined.",
			Required:    true,
			ForceNew:    true,
		},
		"object": {
			Type:        schema.TypeList,
			Description: "Add a pipeline object to the definition.",
			Required:    true,
			Elem:        &schema.Resource{
				Schema: map[string]*schema.Schema{
					"default":			pipelineObjectDefaultSchema(),
					"dynamo_db_data_node":          pipelineObjectDynamoDBDataNodeSchema(),
					"ec2_resource":			pipelineObjectEc2ResourceSchema(),
					"my_sql_data_node":             pipelineObjectMySqlDataNodeSchema(),
					"rds_database":                 pipelineObjectRdsDatabaseSchema(),
					"redshift_copy_activity":       pipelineObjectRedshiftCopyActivitySchema(),
					"redshift_database":		pipelineObjectRedshiftDatabaseSchema(),
					"schedule":			pipelineObjectScheduleSchema(),
					"shell_command_activity":       pipelineObjectShellCommandActivitySchema(),
					"shell_command_precondition":	pipelineObjectShellCommandPreconditionSchema(),
					"sns_alarm":                    pipelineObjectSnsAlarmSchema(),
				},
			},
		},
	}
}

// PIPELINE OBJECTS //

// Default //

// There doesn't appear to be an authoritative definition of what properties
// the Default object can contain...it may be the union of all other types or
// basically be unbounded.
// This covers the examples in the DG and will just have to be revisited as needed.

// pipelineObjectDefaultSchema defines a Default pipeline object.
func pipelineObjectDefaultSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()
	arguments["failure_and_rerun_mode"] = pipelineObjectFailureAndRerunModeArgument()
	arguments["pipeline_log_uri"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	arguments["resource_role"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	arguments["role"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	arguments["schedule"] = pipelineObjectScheduleArgument()
	arguments["schedule_type"] = pipelineObjectScheduleTypeArgument()
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a default configuration.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// Data Nodes //

// pipelineObjectDynamoDBDataNodeSchema defines a DynamoDBDataNode pipeline object.
func pipelineObjectDynamoDBDataNodeSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()

	// Required
	arguments["table_name"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Specify the DynamoDB table.",
		Required:    true,
	}

	// Optional
	arguments["attempt_status"] = pipelineObjectAttemptStatusArgument()
	arguments["attempt_timeout"] = pipelineObjectAttemptTimeoutArgument()
	arguments["data_format"] = &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define DataFormat as used by Hive activities.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
	arguments["depends_on"] = pipelineObjectDependsOnArgument()
	arguments["failure_and_rerun_mode"] = pipelineObjectFailureAndRerunModeArgument()
	arguments["late_after_timeout"] = pipelineObjectLateAfterTimeoutArgument()
	arguments["max_active_instances"] = pipelineObjectMaxActiveInstancesArgument()
	arguments["maximum_retries"] = pipelineObjectMaximumRetriesArgument()
	arguments["on_fail"] = pipelineObjectOnFailArgument()
	arguments["on_late_action"] = pipelineObjectOnLateActionArgument()
	arguments["on_success"] = pipelineObjectOnSuccessArgument()
	arguments["parent"] = pipelineObjectParentArgument()
	arguments["pipeline_log_uri"] = pipelineObjectPipelineLogUriArgument()
	arguments["schedule"] = pipelineObjectScheduleArgument()

	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a DynamoDB data node object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// pipelineObjectMySqlDataNodeSchema defines a MySqlDataNode pipeline object.
func pipelineObjectMySqlDataNodeSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()

	// Required
	arguments["table"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Identify the table within the database.",
		Required:    true,
	}
	
	// Optional
	arguments["attempt_status"] = pipelineObjectAttemptStatusArgument()
	arguments["attempt_timeout"] = pipelineObjectAttemptTimeoutArgument()
	arguments["create_table_sql"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide an SQL statement to create the table.",
		Optional:    true,
	}
	arguments["database"] = &schema.Schema{
		Type:        schema.TypeList,
		Description: "Identify the containing database.",
		Optional:    true,
	}
	arguments["depends_on"] = pipelineObjectDependsOnArgument()
	arguments["failure_and_rerun_mode"] = pipelineObjectFailureAndRerunModeArgument()
	arguments["insert_query"] = &schema.Schema{
	}
	arguments["schedule"] = pipelineObjectScheduleArgument()

	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a MuSQL data node object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// Activities //

// pipelineObjectRedshiftCopyActivitySchema defines a RedshiftCopyActivity object.
func pipelineObjectRedshiftCopyActivitySchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()

	// Required
	arguments["insert_mode"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Specify what to do with any existing overlapping data.",
		Required:    true,
	}
	// Required Group
	arguments["runs_on"] = pipelineObjectRunsOnArgument()
	arguments["worker_group"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Route tasks to specified group if runs_on is not specified.",
		Optional:    true,
	}

	// Optional
	arguments["attempt_status"] = pipelineObjectAttemptStatusArgument()
	arguments["attempt_timeout"] = pipelineObjectAttemptTimeoutArgument()
	arguments["command_options"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide parameters to pass to the Redshift COPY operation.",
		Optional:    true,
	}
	arguments["depends_on"] = pipelineObjectDependsOnArgument()
	arguments["failure_and_rerun_mode"] = pipelineObjectFailureAndRerunModeArgument()
	arguments["input"] = &schema.Schema{
		Type:        schema.TypeList,
	        Description: "Specify the S3, DynamoDB, or Redshift input data node.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
	arguments["late_after_timeout"] = pipelineObjectLateAfterTimeoutArgument()
	arguments["max_active_instances"] = pipelineObjectMaxActiveInstancesArgument()
	arguments["maximum_retries"] = pipelineObjectMaximumRetriesArgument()
	arguments["on_fail"] = pipelineObjectOnFailArgument()
	arguments["on_late_action"] = pipelineObjectOnLateActionArgument()
	arguments["on_success"] = pipelineObjectOnSuccessArgument()
	arguments["output"] = &schema.Schema{
		Type:        schema.TypeList,
	        Description: "Specify the S3 or Redshift output data node.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
	arguments["parent"] = pipelineObjectParentArgument()
	arguments["pipeline_log_uri"] = pipelineObjectPipelineLogUriArgument()
	arguments["precondition"] = pipelineObjectPreconditionArgument()
	arguments["queue"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Identify the Redshift query_group queue to which operations will be added.",
		Optional:    true,
	}
	arguments["report_progress_timeout"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Define time after an activity calls reportProgress when it may be deemed stalled and retried.",
		Optional:    true,
	}
	arguments["retry_delay"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Define the duration between two retry attempts.",
		Optional:    true,
	}
	arguments["schedule"] = pipelineObjectScheduleArgument()
	arguments["schedule_type"] = pipelineObjectScheduleTypeArgument()
	arguments["transform_sql"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Define the SQL to transform the staging table data.",
		Optional:    true,
	}
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a RedshiftCopyActivity pipeline object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// Databases //

// pipelineObjectRdsDatabaseSchema defines an RdsDatabase object.
func pipelineObjectRdsDatabaseSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()

	// Required
	arguments["password"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the password to supply when connecting.",
		Required:    true,
	}
	arguments["rds_instance_id"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the DBInstanceIdentifier of the instance.",
		Required:    true,
	}
	arguments["username"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the username to supply when connecting.",
		Required:    true,
	}

	// Optional
	arguments["database_name"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Name the logical database to which to attach.",
		Optional:    true,
	}
	arguments["jdbc_driver_jar_uri"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide an S3 location for the driver JAR needed to connect.",
		Optional:    true,
	}
	arguments["jdbc_properties"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide properties to set for JDBC connections in the form of A=B.",
		Optional:    true,
	}
	arguments["parent"] = pipelineObjectParentArgument()
	arguments["region"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Specify the AWS region in which the database resides.",
		Optional:    true,
	}
	
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define an RdsDatabase object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// pipelineObjectRedshiftDatabaseSchema defines a RedshiftDatabase object.
func pipelineObjectRedshiftDatabaseSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()

	// Required
	arguments["password"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the password to supply when connecting.",
		Required:    true,
	}
	arguments["username"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the username to supply when connecting.",
		Required:    true,
	}
	// Required Group
	arguments["cluster_id"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Identify the cluster.",
		Optional:    true,
	}
	arguments["connection_string"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide a JDBC endpoint for a Redshift cluster in another account.",
		Optional:    true,
	}

	// Optional
	arguments["database_name"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Name the logical database to which to attach.",
		Optional:    true,
	}
	arguments["jdbc_properties"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide properties to set for JDBC connections in the form of A=B.",
		Optional:    true,
	}
	arguments["parent"] = pipelineObjectParentArgument()
	arguments["region"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Specify the AWS region in which the database resides.",
		Optional:    true,
	}

	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a RedshiftDatabase object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

//
// Actions
//


// pipelineObjectSnsAlarmSchema defines an SnsAlarm pipeline object.
func pipelineObjectSnsAlarmSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()

	// Required
	arguments["message"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the body text of the SNS message.",
		Required:    true,
	}
	arguments["role"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Identify the role used to create the alarm.",
		Optional:    true,
	}
	arguments["subject"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide the subject for the SNS message.",
		Required:    true,
	}
	arguments["topic_arn"] = &schema.Schema{
		Type:        schema.TypeString,
		Description: "Identify the topic to which the message will be sent.",
		Required:    true,
	}


	// Optional
	arguments["parent"] = pipelineObjectParentArgument()

	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define an SnsAlarm object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

//
// Unsorted
//

// pipelineObjectEc2ResoureSchema defines an Ec2Resource pipeline object.
func pipelineObjectEc2ResourceSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()
	arguments["subnet_id"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	arguments["terminate_after"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define an EC2 resource object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// pipelineObjectShellCommandActivitySchema defines a ShellCommandActivity pipeline object.
func pipelineObjectShellCommandActivitySchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()
	arguments["command"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	arguments["depends_on"] = pipelineObjectRefArgument()
	arguments["on_fail"] = pipelineObjectOnFailArgument()
	arguments["on_late_action"] = pipelineObjectOnLateActionArgument()
	arguments["on_success"] = pipelineObjectOnSuccessArgument()
	arguments["precondition"] = pipelineObjectPreconditionArgument()
	arguments["runs_on"] = pipelineObjectRefArgument()
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a shell command activity object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}


// pipelineObjectShellCommandPreconditionSchema defines a ShellCommandPrecondition pipeline object.
func pipelineObjectShellCommandPreconditionSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()
	arguments["command"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a shell command predcondition object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

// pipelineObjectScheduleSchema defines a Schedule pipeline object.
func pipelineObjectScheduleSchema() *schema.Schema {
	arguments := pipelineObjectBaseArguments()
	arguments["period"] = &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	}
	// Either this or the todo start_at should be required
	arguments["start_date_time"] = &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	}
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Define a schedule object.",
		Optional:    true,
		MaxItems:    1,
		Elem:        &schema.Resource{Schema: arguments},
	}
}

//
// SCHEMA HELPERS
//

func pipelineObjectAttemptStatusArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeString,
		Description: "Record the most recently reported status from the remote activity.",
		Optional:    true,
	}
}

func pipelineObjectAttemptTimeoutArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeString,
		Description: "Speicfy timeout for remote work completion.",
		Optional:    true,
	}
}

// pipelineObjectDependsOnArgument defines a common depends_on argument.
func pipelineObjectDependsOnArgument() *schema.Schema {
	return pipelineObjectRefArgument()
}


func pipelineObjectFailureAndRerunModeArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeString,
		Description: "Describe consumer node behavior when dependencies fail or are rerun.",
		Optional:    true,
	}
}

func pipelineObjectLateAfterTimeoutArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeString,
		Description: "Specify time after pipeline start within which the object must start when schedule != ondemand.",
		Optional:    true,
	}
}

func pipelineObjectMaxActiveInstancesArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeInt,
		Description: "Specify the maximum number of concurrent instances (excluding reruns).",
		Optional:    true,
		Default:     1,
	}
}

func pipelineObjectMaximumRetriesArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeInt,
		Description: "Specify maximum number attempt retries on failure.",
		Optional:    true,
	}
}

func pipelineObjectOnFailArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Reference an action to run when current object fails.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
}

func pipelineObjectOnLateActionArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Reference an action to handle late_after_timeout.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
}

func pipelineObjectOnSuccessArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Reference an action to run when current object succeeds.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
}

func pipelineObjectParentArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Reference a parent of the current object from which slots will be inherited.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
}

func pipelineObjectPipelineLogUriArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeString,
		Description: "Provide an S3 URI (such as 's3://BucketName/Key/') for uploading logs for the pipeline.",
		Optional:    true,
	}
}

func pipelineObjectPreconditionArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Reference a precondition which must be met prior to this node being READY.",
		Optional:    true,
		MaxItems:    1,
		Elem:        pipelineObjectRefResource(),
	}
}

func pipelineObjectRunsOnArgument() *schema.Schema {
	return pipelineObjectRefArgument()
}

// pipelineObjectScheduleArgument defines a common depends_on argument.
func pipelineObjectScheduleArgument() *schema.Schema {
	return pipelineObjectRefArgument()
}

func pipelineObjectScheduleTypeArgument() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeString,
		Description: "Indicate the type of scheduler which launches the pipeline.",
		Optional:    true,
	}	
}

// pipelineObjectBaseArguments defines some common arguments used across object types.
func pipelineObjectBaseArguments() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"id": {
			Type:     schema.TypeString,
			Required: true,
		},
		"name": {
			Type:     schema.TypeString,
			Required: true,
		},
	}
}

// TODO: Move away from this
// pipelineObjectRefArgument defines an argument that has a ref string child.
func pipelineObjectRefArgument() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MaxItems: 1,
		Elem:     pipelineObjectRefResource(),
	}
}

func pipelineObjectRefResource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"ref": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}


//
// TRANSITION HANDLERS (not sure if there's a conventional name for this)
//

// resourceAwsDataPipelineDefinitionPut pushes the definition to AWS.
// This is used for both creation and updating of a definition.
func resourceAwsDataPipelineDefinitionPut(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).datapipelineconn

	pipelineID := d.Get("pipeline_id").(string)
	pipelineObjects, err := buildPipelineObjects(d.Get("object").([]interface{}))
	if err != nil {
		return err
	}

	input := &datapipeline.PutPipelineDefinitionInput{
		PipelineId: aws.String(pipelineID),
		PipelineObjects: pipelineObjects,
	}

	_, err = conn.PutPipelineDefinition(input)
	if isAWSErr(err, datapipeline.ErrCodePipelineNotFoundException, "") || isAWSErr(err, datapipeline.ErrCodePipelineDeletedException, "") {
		log.Printf("[WARN] DataPipeline (%s) not found, removing from state", pipelineID)
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error %v putting DataPipeline (%s) Definition: %v", err, pipelineID, input)
	}

	d.SetId(pipelineID)
	return resourceAwsDataPipelineDefinitionRead(d, meta)
}

var dataPipelineScheduleTypeList = []string{
	"cron",
	"ondemand",
	"timeseries",
}

var dataPipelineFailureAndRerunModeList = []string{
	"cascade",
	"none",
}


func resourceAwsDataPipelineDefinitionRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).datapipelineconn

	_, err := resourceAwsDataPipelineDefinitionRetrieve(d.Id(), conn)
	if isAWSErr(err, datapipeline.ErrCodePipelineNotFoundException, "") || isAWSErr(err, datapipeline.ErrCodePipelineDeletedException, "") {
		log.Printf("[WARN] DataPipeline (%s) Definition not found, removing from state", d.Id())
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error getting DataPipeline (%s) Definition: %s", d.Id(), err)
	}

	// TODO: Read existing pipeline objects

	return nil
}

func resourceAwsDataPipelineDefinitionImport(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	d.Set("pipeline_id", d.Id())
	return []*schema.ResourceData{d}, nil
}

func resourceAwsDataPipelineDefinitionRetrieve(id string, conn *datapipeline.DataPipeline) (*datapipeline.GetPipelineDefinitionOutput, error) {
	opts := datapipeline.GetPipelineDefinitionInput{
		PipelineId: aws.String(id),
	}
	log.Printf("[DEBUG] Data Pipeline describe configuration: %#v", opts)

	resp, err := conn.GetPipelineDefinition(&opts)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

//
// CONVERTERS
//

// buildPipelineObjects converts the list of configured objects into suitable forms for the AWS SDK.
// This function handles some type coercion and list construction and passes on the object creation.
func buildPipelineObjects(objs []interface{}) ([]*datapipeline.PipelineObject, error) {
	pipelineObjects := make([]*datapipeline.PipelineObject, len(objs))
	for i, o := range objs {
		po, err := buildPipelineObject(o.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		pipelineObjects[i] = po
	}
	return pipelineObjects, nil
}

func buildPipelineObject(o map[string]interface{}) (*datapipeline.PipelineObject, error) {
	oCount := 0
	for oType, oContent := range o {
		defList := oContent.([]interface{})
		if len(defList) == 0 {
			continue
		}
		if len(defList) > 1 {
			return nil, fmt.Errorf("Unexpected definition content: %v", defList)
		}
		oCount++
		if (oCount > 1) {
			return nil, fmt.Errorf("Too many definitions with one PipelineObject: %v", o)
		}
		def := defList[0].(map[string]interface{})
		// def includes id and name which will be removed, but we also want type
		fields := []*datapipeline.Field{
			&datapipeline.Field{
				Key: aws.String("type"),
				StringValue: snakeToCamel(oType, true),
			},
		}
		for k, v := range def {
			if k == "id" || k == "name" {
				continue
			}
			field, err := expandPipelineField(k, v)
			if err != nil {
				return nil, err
			}
			if field != nil {
				fields = append(fields, field)
			}
		}
		return &datapipeline.PipelineObject{
			Id:     aws.String(def["id"].(string)),
			Name:   aws.String(def["name"].(string)),
			Fields: fields,
		}, nil
	}
	return nil, fmt.Errorf("Should never be reached")
}

func expandPipelineField(k string, v interface{}) (*datapipeline.Field, error) {
	switch v.(type) {
	case string:
		if strVal := v.(string); strVal != "" {
			key := snakeToCamel(k, false)
			if *key == "password" {
				*key = "*password"
			}
			return &datapipeline.Field{
				Key: key,
				StringValue: &strVal,
			}, nil
		} else {
			return nil, nil
		}
	case int:
		return &datapipeline.Field{
			Key: snakeToCamel(k, false),
			StringValue: aws.String(strconv.Itoa(v.(int))),
		}, nil
	}

	
	if a, ok := v.([]interface{}); ok {
		if refVal, err := extractRef(a); err != nil {
			return nil, fmt.Errorf("When converting: %s-%v; %v", k, v, err)
		} else {
			if refVal != nil {
				return &datapipeline.Field{
					Key: snakeToCamel(k, false),
					RefValue: refVal,
				}, nil
			} else {
				return nil, nil
			}
		}
	} else {
		return nil, fmt.Errorf("Unsupported type for %s, : %#v", k, v)
	}
}

func extractRef(v []interface{}) (*string, error) {
	if len(v) == 0 {
		return nil, nil
	}
	if len(v) > 1 {
		return nil, fmt.Errorf("Bad ref value length for %v", v)
	}
	vMap, ok := v[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Bad ref value map for %v", v)
	}
	val, ok := vMap["ref"].(string)
	if !ok {
		return nil, fmt.Errorf("Bad ref value stirng for %v", v)
	}
	return &val, nil
}

func snakeToCamel(snake string, upNext bool) (*string) {
	var sb strings.Builder
	
	for _, v := range snake {
		switch v {
		case '_':
			upNext = true
		default:
			if upNext {
				sb.WriteString(strings.ToUpper(string(v)))
				upNext = false
			} else {
				sb.WriteRune(v)
			}
		}
	}
	camel := sb.String()
	return &camel
}
