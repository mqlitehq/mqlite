package hranaV2

import (
	"fmt"

	"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana"
)

// Validate the correspondence before indexing or interpreting any success.
// Close acknowledgements are valid when the matching request closes a stream.
func validateAcknowledgement(request *hrana.PipelineRequest, response *hrana.PipelineResponse) error {
	if len(response.Results) != len(request.Requests) {
		return hrana.InvalidAcknowledgement(fmt.Errorf("pipeline returned %d results for %d requests", len(response.Results), len(request.Requests)))
	}
	for i, result := range response.Results {
		if result.Type == "error" && result.Error != nil && result.Response == nil {
			continue // Retain the pinned driver's definite protocol rejection.
		}
		if result.Type != "ok" || result.Error != nil || result.Response == nil || result.Response.Type != request.Requests[i].Type {
			return hrana.InvalidAcknowledgement(fmt.Errorf("pipeline result %d does not acknowledge its request", i))
		}
		if batch := request.Requests[i].Batch; batch != nil {
			result.Response.RequestedBatch = batch
		}
	}
	return nil
}
