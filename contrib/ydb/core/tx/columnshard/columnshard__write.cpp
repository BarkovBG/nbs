#include "columnshard_impl.h"
#include "blobs_action/transaction/tx_write.h"
#include "blobs_action/transaction/tx_draft.h"
#include "counters/columnshard.h"
#include "operations/slice_builder.h"
#include "operations/write_data.h"

#include <contrib/ydb/core/tx/conveyor/usage/service.h>
#include <contrib/ydb/core/tx/data_events/events.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

void TColumnShard::OverloadWriteFail(const EOverloadStatus overloadReason, const NEvWrite::TWriteData& writeData, std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx) {
    IncCounter(COUNTER_WRITE_FAIL);
    switch (overloadReason) {
        case EOverloadStatus::Disk:
            IncCounter(COUNTER_OUT_OF_SPACE);
            break;
        case EOverloadStatus::InsertTable:
            IncCounter(COUNTER_WRITE_OVERLOAD);
            CSCounters.OnOverloadInsertTable(writeData.GetSize());
            break;
        case EOverloadStatus::ShardTxInFly:
            IncCounter(COUNTER_WRITE_OVERLOAD);
            CSCounters.OnOverloadShardTx(writeData.GetSize());
            break;
        case EOverloadStatus::ShardWritesInFly:
            IncCounter(COUNTER_WRITE_OVERLOAD);
            CSCounters.OnOverloadShardWrites(writeData.GetSize());
            break;
        case EOverloadStatus::ShardWritesSizeInFly:
            IncCounter(COUNTER_WRITE_OVERLOAD);
            CSCounters.OnOverloadShardWritesSize(writeData.GetSize());
            break;
        case EOverloadStatus::None:
            Y_ABORT("invalid function usage");
    }

    LOG_S_INFO("Write (overload) " << writeData.GetSize() << " bytes into pathId " << writeData.GetWriteMeta().GetTableId()
        << " overload reason: [" << overloadReason << "]"
        << " at tablet " << TabletID());

    ctx.Send(writeData.GetWriteMeta().GetSource(), event.release());
}

TColumnShard::EOverloadStatus TColumnShard::CheckOverloaded(const ui64 tableId) const {
    if (IsAnyChannelYellowStop()) {
        return EOverloadStatus::Disk;
    }

    if (InsertTable && InsertTable->IsOverloadedByCommitted(tableId)) {
        return EOverloadStatus::InsertTable;
    }

    ui64 txLimit = Settings.OverloadTxInFlight;
    ui64 writesLimit = Settings.OverloadWritesInFlight;
    ui64 writesSizeLimit = Settings.OverloadWritesSizeInFlight;
    if (txLimit && Executor()->GetStats().TxInFly > txLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "shard_overload")("reason", "tx_in_fly")("sum", Executor()->GetStats().TxInFly)("limit", txLimit);
        return EOverloadStatus::ShardTxInFly;
    }
    if (writesLimit && WritesMonitor.GetWritesInFlight() > writesLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "shard_overload")("reason", "writes_in_fly")("sum", WritesMonitor.GetWritesInFlight())("limit", writesLimit);
        return EOverloadStatus::ShardWritesInFly;
    }
    if (writesSizeLimit && WritesMonitor.GetWritesSizeInFlight() > writesSizeLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "shard_overload")("reason", "writes_size_in_fly")("sum", WritesMonitor.GetWritesSizeInFlight())("limit", writesSizeLimit);
        return EOverloadStatus::ShardWritesSizeInFly;
    }
    return EOverloadStatus::None;
}

void TColumnShard::Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& ev, const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("event", "TEvWriteBlobsResult");

    auto& putResult = ev->Get()->GetPutResult();
    OnYellowChannels(putResult);
    NOlap::TWritingBuffer& wBuffer = ev->Get()->MutableWritesBuffer();
    auto baseAggregations = wBuffer.GetAggregations();
    wBuffer.InitReplyReceived(TMonotonic::Now());

    auto wg = WritesMonitor.FinishWrite(wBuffer.GetSumSize(), wBuffer.GetAggregations().size());

    for (auto&& aggr : baseAggregations) {
        const auto& writeMeta = aggr->GetWriteData()->GetWriteMeta();

        if (!TablesManager.IsReadyForWrite(writeMeta.GetTableId())) {
            ACFL_ERROR("event", "absent_pathId")("path_id", writeMeta.GetTableId())("has_index", TablesManager.HasPrimaryIndex());
            IncCounter(COUNTER_WRITE_FAIL);

            auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, NKikimrTxColumnShard::EResultStatus::ERROR);
            ctx.Send(writeMeta.GetSource(), result.release());
            CSCounters.OnFailedWriteResponse(EWriteFailReason::NoTable);
            wBuffer.RemoveData(aggr, StoragesManager->GetInsertOperator());
            continue;
        }

        if (putResult.GetPutStatus() != NKikimrProto::OK) {
            CSCounters.OnWritePutBlobsFail(TMonotonic::Now() - writeMeta.GetWriteStartInstant());
            IncCounter(COUNTER_WRITE_FAIL);

            auto errCode = NKikimrTxColumnShard::EResultStatus::STORAGE_ERROR;
            if (putResult.GetPutStatus() == NKikimrProto::TIMEOUT || putResult.GetPutStatus() == NKikimrProto::DEADLINE) {
                errCode = NKikimrTxColumnShard::EResultStatus::TIMEOUT;
            } else if (putResult.GetPutStatus() == NKikimrProto::TRYLATER || putResult.GetPutStatus() == NKikimrProto::OUT_OF_SPACE) {
                errCode = NKikimrTxColumnShard::EResultStatus::OVERLOADED;
            } else if (putResult.GetPutStatus() == NKikimrProto::CORRUPTED) {
                errCode = NKikimrTxColumnShard::EResultStatus::ERROR;
            }

            if (writeMeta.HasLongTxId()) {
                auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, errCode);
                ctx.Send(writeMeta.GetSource(), result.release());
            } else {
                auto operation = OperationsManager->GetOperation((TWriteId)writeMeta.GetWriteId());
                Y_ABORT_UNLESS(operation);
                auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), operation->GetTxId(), NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, "put data fails");
                ctx.Send(writeMeta.GetSource(), result.release());
            }
            CSCounters.OnFailedWriteResponse(EWriteFailReason::PutBlob);
            wBuffer.RemoveData(aggr, StoragesManager->GetInsertOperator());
        } else {
            const TMonotonic now = TMonotonic::Now();
            CSCounters.OnWritePutBlobsSuccess(now - writeMeta.GetWriteStartInstant());
            CSCounters.OnWriteMiddle1PutBlobsSuccess(now - writeMeta.GetWriteMiddle1StartInstant());
            CSCounters.OnWriteMiddle2PutBlobsSuccess(now - writeMeta.GetWriteMiddle2StartInstant());
            CSCounters.OnWriteMiddle3PutBlobsSuccess(now - writeMeta.GetWriteMiddle3StartInstant());
            CSCounters.OnWriteMiddle4PutBlobsSuccess(now - writeMeta.GetWriteMiddle4StartInstant());
            CSCounters.OnWriteMiddle5PutBlobsSuccess(now - writeMeta.GetWriteMiddle5StartInstant());
            CSCounters.OnWriteMiddle6PutBlobsSuccess(now - writeMeta.GetWriteMiddle6StartInstant());
            LOG_S_DEBUG("Write (record) into pathId " << writeMeta.GetTableId()
                << (writeMeta.GetWriteId() ? (" writeId " + ToString(writeMeta.GetWriteId())).c_str() : "") << " at tablet " << TabletID());

        }
    }
    Execute(new TTxWrite(this, ev), ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvWriteDraft::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWriteDraft(this, ev->Get()->WriteController), ctx);
}

void TColumnShard::Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    CSCounters.OnStartWriteRequest();
    LastAccessTime = TAppData::TimeProvider->Now();

    const auto& record = Proto(ev->Get());
    const ui64 tableId = record.GetTableId();
    const ui64 writeId = record.GetWriteId();
    const TString dedupId = record.GetDedupId();
    const auto source = ev->Sender;

    NEvWrite::TWriteMeta writeMeta(writeId, tableId, source);
    writeMeta.SetDedupId(dedupId);
    Y_ABORT_UNLESS(record.HasLongTxId());
    writeMeta.SetLongTxId(NLongTxService::TLongTxId::FromProto(record.GetLongTxId()));
    writeMeta.SetWritePartId(record.GetWritePartId());

    const auto returnFail = [&](const NColumnShard::ECumulativeCounters signalIndex) {
        IncCounter(signalIndex);

        ctx.Send(source, std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, NKikimrTxColumnShard::EResultStatus::ERROR));
        return;
    };

    if (!AppDataVerified().ColumnShardConfig.GetWritingEnabled()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_writing")("reason", "disabled");
        CSCounters.OnFailedWriteResponse(EWriteFailReason::Disabled);
        return returnFail(COUNTER_WRITE_FAIL);
    }

    if (!TablesManager.IsReadyForWrite(tableId)) {
        LOG_S_NOTICE("Write (fail) into pathId:" << writeMeta.GetTableId() << (TablesManager.HasPrimaryIndex()? "": " no index")
            << " at tablet " << TabletID());

        CSCounters.OnFailedWriteResponse(EWriteFailReason::NoTable);
        return returnFail(COUNTER_WRITE_FAIL);
    }

    const auto& snapshotSchema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
    auto arrowData = std::make_shared<TProtoArrowData>(snapshotSchema);
    if (!arrowData->ParseFromProto(record)) {
        LOG_S_ERROR("Write (fail) " << record.GetData().size() << " bytes into pathId " << writeMeta.GetTableId()
            << " at tablet " << TabletID());
        CSCounters.OnFailedWriteResponse(EWriteFailReason::IncorrectSchema);
        return returnFail(COUNTER_WRITE_FAIL);
    }

    NEvWrite::TWriteData writeData(writeMeta, arrowData, snapshotSchema->GetIndexInfo().GetReplaceKey(), StoragesManager->GetInsertOperator()->StartWritingAction("WRITING"));
    auto overloadStatus = CheckOverloaded(tableId);
    if (overloadStatus != EOverloadStatus::None) {
        std::unique_ptr<NActors::IEventBase> result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeData.GetWriteMeta(), NKikimrTxColumnShard::EResultStatus::OVERLOADED);
        OverloadWriteFail(overloadStatus, writeData, std::move(result), ctx);
        CSCounters.OnFailedWriteResponse(EWriteFailReason::Overload);
    } else {
        if (ui64 writeId = (ui64)HasLongTxWrite(writeMeta.GetLongTxIdUnsafe(), writeMeta.GetWritePartId())) {
            LOG_S_DEBUG("Write (duplicate) into pathId " << writeMeta.GetTableId()
                << " longTx " << writeMeta.GetLongTxIdUnsafe().ToString()
                << " at tablet " << TabletID());

            IncCounter(COUNTER_WRITE_DUPLICATE);

            auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
                TabletID(), writeMeta, writeId, NKikimrTxColumnShard::EResultStatus::SUCCESS);
            ctx.Send(writeMeta.GetSource(), result.release());
            CSCounters.OnFailedWriteResponse(EWriteFailReason::LongTxDuplication);
            return;
        }

        WritesMonitor.RegisterWrite(writeData.GetSize());

        LOG_S_DEBUG("Write (blob) " << writeData.GetSize() << " bytes into pathId " << writeMeta.GetTableId()
            << (writeMeta.GetWriteId()? (" writeId " + ToString(writeMeta.GetWriteId())).c_str() : " ")
            << WritesMonitor.DebugString()
            << " at tablet " << TabletID());
        writeData.MutableWriteMeta().SetWriteMiddle1StartInstant(TMonotonic::Now());
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(TabletID(), SelfId(), BufferizationWriteActorId, std::move(writeData));
        NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
    }
}

void TColumnShard::Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("event", "TEvWrite");

    const auto& record = ev->Get()->Record;
    const ui64 txId = ev->Get()->GetTxId();
    const auto source = ev->Sender;

    if (record.GetOperations().size() != 1) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "only single operation is supported");
        ctx.Send(source, result.release());
        return;
    }

    const auto& operation = record.GetOperations()[0];

    if (operation.GetType() != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "only REPLACE operation is supported");
        ctx.Send(source, result.release());
        return;
    }

    if (!operation.GetTableId().HasSchemaVersion()) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "schema version not set");
        ctx.Send(source, result.release());
        return;
    }

    auto schema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchema(operation.GetTableId().GetSchemaVersion());
    if (!schema) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "unknown schema version");
        ctx.Send(source, result.release());
        return;
    }

    const auto tableId = operation.GetTableId().GetTableId();

    if (!TablesManager.IsReadyForWrite(tableId)) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, "table not writable");
        ctx.Send(source, result.release());
        return;
    }

    auto arrowData = std::make_shared<TArrowData>(schema);
    if (!arrowData->Parse(operation, NEvWrite::TPayloadHelper<NEvents::TDataEvents::TEvWrite>(*ev->Get()))) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "parsing data error");
        ctx.Send(source, result.release());
    }

    auto overloadStatus = CheckOverloaded(tableId);
    if (overloadStatus != EOverloadStatus::None) {
        NEvWrite::TWriteData writeData(NEvWrite::TWriteMeta(0, tableId, source), arrowData, nullptr, nullptr);
        std::unique_ptr<NActors::IEventBase> result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), txId, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, "overload data error");
        OverloadWriteFail(overloadStatus, writeData, std::move(result), ctx);
        return;
    }

    auto wg = WritesMonitor.RegisterWrite(arrowData->GetSize());

    auto writeOperation = OperationsManager->RegisterOperation(txId);
    Y_ABORT_UNLESS(writeOperation);
    writeOperation->Start(*this, tableId, arrowData, source, ctx);
}

}
