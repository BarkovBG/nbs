#include "constructor.h"
#include <contrib/ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

void TBlobsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    Source->MutableStageData().AddBlobs(ExtractBlobsData());
    AFL_VERIFY(Step->GetNextStep());
    auto task = std::make_shared<TStepAction>(Source, Step->GetNextStep(), Context->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

bool TBlobsFetcherTask::DoOnError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())("scan_actor_id", Context->GetCommonContext()->GetScanActorId())
        ("status", status.GetErrorMessage())("status_code", status.GetStatus());
    NActors::TActorContext::AsActorContext().Send(Context->GetCommonContext()->GetScanActorId(), 
        std::make_unique<NConveyor::TEvExecution::TEvTaskProcessedResult>(TConclusionStatus::Fail("cannot read blob range " + range.ToString())));
    return false;
}

}
