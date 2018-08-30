package aws

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/datapipeline"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func init() {
	resource.AddTestSweepers("aws_datapipeline", &resource.Sweeper{
		Name: "aws_datapipeline",
		F:    testSweepDataPipeline,
	})
}

func testSweepDataPipeline(region string) error {
	client, err := sharedClientForRegion(region)
	if err != nil {
		return fmt.Errorf("error getting client: %s", err)
	}
	conn := client.(*AWSClient).datapipelineconn

	prefixes := []string{
		"foobarbaz-test-terraform-",
		"terraform-",
		"tf-",
	}

	err = conn.ListPipelinesPages(&datapipeline.ListPipelinesInput{}, func(out *datapipeline.ListPipelinesOutput, lastPage bool) bool {
		for _, pipelineIDName := range out.PipelineIdList {
			hasPrefix := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(*pipelineIDName.Name, prefix) {
					hasPrefix = true
				}
			}
			if !hasPrefix {
				continue
			}
			log.Printf("[INFO] Deleting DataPipeline: %s", *pipelineIDName.Name)

			_, err := conn.DeletePipeline(&datapipeline.DeletePipelineInput{
				PipelineId: pipelineIDName.Id,
			})
			if err != nil {
				log.Printf("[ERROR] Failed to delete DataPipeline  %s: %s",
					*pipelineIDName.Name, err)
				continue
			}

		}
		return !lastPage
	})
	if err != nil {
		return fmt.Errorf("Error retrieving DataPipeline: %s", err)
	}

	return nil
}

func TestAccAWSDataPipeline_basic(t *testing.T) {

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSDataPipelineDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSDataPipelineConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSDataPipelineExists("aws_db_instance.bar"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "allocated_storage", "10"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "engine", "mysql"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "license_model", "general-public-license"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "instance_class", "db.t2.micro"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "name", "baz"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "username", "foo"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "parameter_group_name", "default.mysql5.6"),
					resource.TestCheckResourceAttr(
						"aws_datapipeline.bar", "enabled_cloudwatch_logs_exports.#", "0"),
					resource.TestCheckResourceAttrSet("aws_datapipeline.bar", "hosted_zone_id"),
					resource.TestCheckResourceAttrSet("aws_datapipeline.bar", "ca_cert_identifier"),
					resource.TestCheckResourceAttrSet(
						"aws_datapipeline.bar", "resource_id"),
				),
			},
		},
	})
}

func testAccCheckAWSDataPipelineDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*AWSClient).datapipelineconn

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aws_datapipeline" {
			continue
		}

		// Try to find the Group
		var err error
		resp, err := conn.DescribePipelines(
			&datapipeline.DescribePipelinesInput{
				PipelineIds: []*string{
					aws.String(rs.Primary.ID),
				},
			})

		if ae, ok := err.(awserr.Error); ok && ae.Code() == datapipeline.ErrCodePipelineNotFoundException {
			continue
		}

		if err == nil {
			if len(resp.PipelineDescriptionList) != 0 &&
				*resp.PipelineDescriptionList[0].PipelineId == rs.Primary.ID {
				return fmt.Errorf("DataPipeline still exists")
			}
		}

		// Verify the error
		newerr, ok := err.(awserr.Error)
		if !ok {
			return err
		}
		if newerr.Code() != datapipeline.ErrCodePipelineNotFoundException {
			return err
		}
	}

	return nil
}

func testAccCheckAWSDataPipelineExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No DB Instance ID is set")
		}

		conn := testAccProvider.Meta().(*AWSClient).datapipelineconn

		opts := datapipeline.DescribePipelinesInput{
			PipelineIds: []*string{aws.String(rs.Primary.ID)},
		}

		resp, err := conn.DescribePipelines(&opts)

		if err != nil {
			return err
		}

		if len(resp.PipelineDescriptionList) != 1 ||
			*resp.PipelineDescriptionList[0].PipelineId != rs.Primary.ID {
			return fmt.Errorf("DataPipeline not found")
		}

		return nil
	}
}

// DataPipeline names cannot collide, and deletion takes so long, that making the
// name a bit random helps so able we can kill a test that's just waiting for a
// delete and not be blocked on kicking off another one.
var testAccAWSDataPipelineConfig = `
resource "aws_datapipeline" "bar" {

}`
