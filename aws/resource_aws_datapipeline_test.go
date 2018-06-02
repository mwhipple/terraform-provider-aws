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
