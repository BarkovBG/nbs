#include <contrib/ydb/library/yql/parser/pg_wrapper/interface/interface.h>
#include <contrib/ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <contrib/ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <contrib/ydb/library/yql/minikql/computation/presort_impl.h>
#include <contrib/ydb/library/yql/minikql/mkql_node_cast.h>
#include <contrib/ydb/library/yql/minikql/mkql_alloc.h>
#include <contrib/ydb/library/yql/minikql/mkql_buffer.h>
#include <contrib/ydb/library/yql/minikql/mkql_node_builder.h>
#include <contrib/ydb/library/yql/minikql/mkql_string_util.h>
#include <contrib/ydb/library/yql/minikql/mkql_type_builder.h>
#include <contrib/ydb/library/yql/public/udf/arrow/block_reader.h>
#include <contrib/ydb/library/yql/public/udf/arrow/block_builder.cpp>
#include <contrib/ydb/library/yql/parser/pg_catalog/catalog.h>
#include <contrib/ydb/library/yql/providers/common/codec/yql_codec_buf.h>
#include <contrib/ydb/library/yql/providers/common/codec/yql_codec_results.h>
#include <contrib/ydb/library/yql/public/udf/udf_value_builder.h>
#include <contrib/ydb/library/yql/utils/fp_bits.h>
#include <library/cpp/yson/detail.h>
#include <util/string/split.h>
#include <util/system/getpid.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#define Sort PG_Sort
#define Unique PG_Unique
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_conversion_d.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/lsyscache.h"
#include "utils/datetime.h"
#include "utils/typcache.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "thread_inits.h"

#undef Abs
#undef Min
#undef Max
#undef TypeName
#undef SortBy
#undef Sort
#undef Unique
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
//#undef ERROR
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t
}

#include "arrow.h"

namespace NYql {

using namespace NKikimr::NMiniKQL;

TVPtrHolder TVPtrHolder::Instance;

// use 'false' for native format
static __thread bool NeedCanonizeFp = false;

struct TMainContext {
    MemoryContextData Data;
    MemoryContextData ErrorData;
    MemoryContext PrevCurrentMemoryContext = nullptr;
    MemoryContext PrevErrorContext = nullptr;
    MemoryContext PrevCacheMemoryContext = nullptr;
    RecordCacheState CurrentRecordCacheState = { NULL, NULL, NULL, 0, 0, INVALID_TUPLEDESC_IDENTIFIER };
    RecordCacheState PrevRecordCacheState;
    TimestampTz StartTimestamp;
    pg_stack_base_t PrevStackBase;
    TString LastError;
};

NUdf::TUnboxedValue CreatePgString(i32 typeLen, ui32 targetTypeId, TStringBuf data) {
    // typname => 'cstring', typlen => '-2'
    // typname = > 'text', typlen => '-1'
    // typname => 'name', typlen => NAMEDATALEN
    Y_UNUSED(targetTypeId); // todo: verify typeLen
    switch (typeLen) {
    case -1:
        return PointerDatumToPod((Datum)MakeVar(data));
    case -2:
        return PointerDatumToPod((Datum)MakeCString(data));
    default:
        return PointerDatumToPod((Datum)MakeFixedString(data, typeLen));
    }
}

void *MkqlAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + sizeof(TMkqlPAllocHeader);
    auto header = (TMkqlPAllocHeader*)MKQLAllocWithSize(fullSize, EMemorySubPool::Default);
    header->Size = size;
    header->U.Entry.Link(TlsAllocState->CurrentPAllocList);
    header->Self = context;
    return header + 1;
}

void MkqlAllocSetFree(MemoryContext context, void* pointer) {
    if (pointer) {
        auto header = ((TMkqlPAllocHeader*)pointer) - 1;
        // remove this block from list
        header->U.Entry.Unlink();
        auto fullSize = header->Size + sizeof(TMkqlPAllocHeader);
        MKQLFreeWithSize(header, fullSize, EMemorySubPool::Default);
    }
}

void* MkqlAllocSetRealloc(MemoryContext context, void* pointer, Size size) {
    if (!size) {
        MkqlAllocSetFree(context, pointer);
        return nullptr;
    }

    auto ret = MkqlAllocSetAlloc(context, size);
    if (pointer) {
        auto header = ((TMkqlPAllocHeader*)pointer) - 1;
        memmove(ret, pointer, header->Size);
        MkqlAllocSetFree(context, pointer);
    }

    return ret;
}

void MkqlAllocSetReset(MemoryContext context) {
}

void MkqlAllocSetDelete(MemoryContext context) {
}

Size MkqlAllocSetGetChunkSpace(MemoryContext context, void* pointer) {
    return 0;
}

bool MkqlAllocSetIsEmpty(MemoryContext context) {
    return false;
}

void MkqlAllocSetStats(MemoryContext context,
    MemoryStatsPrintFunc printfunc, void *passthru,
    MemoryContextCounters *totals,
    bool print_to_stderr) {
}

void MkqlAllocSetCheck(MemoryContext context) {
}

const MemoryContextMethods MkqlMethods = {
    MkqlAllocSetAlloc,
    MkqlAllocSetFree,
    MkqlAllocSetRealloc,
    MkqlAllocSetReset,
    MkqlAllocSetDelete,
    MkqlAllocSetGetChunkSpace,
    MkqlAllocSetIsEmpty,
    MkqlAllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
    ,MkqlAllocSetCheck
#endif
};

Datum MakeArrayOfText(const TVector<TString>& arr) {
    TVector<Datum> elems(arr.size());
    for (size_t i = 0; i < elems.size(); ++i) {
        elems[i] = (Datum)MakeVar(arr[i]);
    }

    auto ret = construct_array(elems.data(), (int)arr.size(), TEXTOID, -1, false, 'i');
    for (size_t i = 0; i < elems.size(); ++i) {
        pfree((void*)elems[i]);
    }

    return (Datum)ret;
}

class TPgConst : public TMutableComputationNode<TPgConst> {
    typedef TMutableComputationNode<TPgConst> TBaseComputation;
public:
    TPgConst(TComputationMutables& mutables, ui32 typeId, const std::string_view& value, IComputationNode* typeMod)
        : TBaseComputation(mutables)
        , TypeId(typeId)
        , Value(value)
        , TypeMod(typeMod)
        , TypeDesc(NPg::LookupType(TypeId))
    {
        Zero(FInfo);
        ui32 inFuncId = TypeDesc.InFuncId;
        if (TypeDesc.TypeId == TypeDesc.ArrayTypeId) {
            inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
        }

        Y_ENSURE(inFuncId);
        fmgr_info(inFuncId, &FInfo);
        Y_ENSURE(!FInfo.fn_retset);
        Y_ENSURE(FInfo.fn_addr);
        Y_ENSURE(FInfo.fn_nargs >=1 && FInfo.fn_nargs <= 3);
        TypeIOParam = MakeTypeIOParam(TypeDesc);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        i32 typeMod = -1;
        if (TypeMod) {
            typeMod = DatumGetInt32(ScalarDatumFromPod(TypeMod->GetValue(compCtx)));
        }

        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        FmgrInfo copyFmgrInfo = FInfo;
        callInfo->flinfo = &copyFmgrInfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)Value.c_str(), false };
        callInfo->args[1] = { ObjectIdGetDatum(TypeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(typeMod), false };

        TPAllocScope call;
        auto ret = FInfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return AnyDatumToPod(ret, TypeDesc.PassByValue);
    }

private:
    void RegisterDependencies() const final {
        if (TypeMod) {
            DependsOn(TypeMod);
        }
    }

    const ui32 TypeId;
    const TString Value;
    IComputationNode* const TypeMod;
    const NPg::TTypeDesc TypeDesc;
    FmgrInfo FInfo;
    ui32 TypeIOParam;
};

class TPgInternal0 : public TMutableComputationNode<TPgInternal0> {
    typedef TMutableComputationNode<TPgInternal0> TBaseComputation;
public:
    TPgInternal0(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        return ScalarDatumToPod(PointerGetDatum(nullptr));
    }

private:
    void RegisterDependencies() const final {
    }
};

class TPgTableContent : public TMutableComputationNode<TPgTableContent> {
    typedef TMutableComputationNode<TPgTableContent> TBaseComputation;
public:
    TPgTableContent(
        TComputationMutables& mutables,
        const std::string_view& cluster,
        const std::string_view& table,
        TType* returnType)
        : TBaseComputation(mutables)
        , Cluster_(cluster)
        , Table_(table)
        , ItemType_(AS_TYPE(TStructType, AS_TYPE(TListType, returnType)->GetItemType()))
    {
        YQL_ENSURE(Cluster_ == "pg_catalog" || Cluster_ == "information_schema");
        if (Cluster_ == "pg_catalog") {
            if (Table_ == "pg_type") {
                static const std::pair<const char*, TPgTypeFiller> AllPgTypeFillers[] = {
                    {"oid", [](const NPg::TTypeDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(desc.TypeId)); }},
                    {"typname", [](const NPg::TTypeDesc& desc) { return PointerDatumToPod((Datum)(MakeFixedString(desc.Name, NAMEDATALEN))); }},
                    {"typinput", [](const NPg::TTypeDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(desc.InFuncId)); }},
                    {"typnamespace", [](const NPg::TTypeDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(1)); }},
                    {"typtype", [](const NPg::TTypeDesc& desc) { return ScalarDatumToPod(CharGetDatum(desc.TypType)); }},
                };

                ApplyFillers(AllPgTypeFillers, Y_ARRAY_SIZE(AllPgTypeFillers), PgTypeFillers_);
            } else if (Table_ == "pg_database") {
                static const std::pair<const char*, TPgDatabaseFiller> AllPgDatabaseFillers[] = {
                    {"oid", [](ui32 index) { return ScalarDatumToPod(ObjectIdGetDatum(index)); }},
                    {"datdba", [](ui32) { return ScalarDatumToPod(ObjectIdGetDatum(1)); }},
                    {"datistemplate", [](ui32 index) { return ScalarDatumToPod(BoolGetDatum(index < 3)); }},
                    {"datallowconn", [](ui32 index) { return ScalarDatumToPod(BoolGetDatum(index != 2)); }},
                    {"datname", [](ui32 index) { return PointerDatumToPod((Datum)(MakeFixedString(
                        index == 1 ? "template1" : (index == 2 ? "template0" : "postgres"), NAMEDATALEN))); }},
                    {"encoding", [](ui32) { return ScalarDatumToPod(Int32GetDatum(PG_UTF8)); }},
                };

                ApplyFillers(AllPgDatabaseFillers, Y_ARRAY_SIZE(AllPgDatabaseFillers), PgDatabaseFillers_);
            } else if (Table_ == "pg_tablespace") {
                static const std::pair<const char*, TPgTablespaceFiller> AllPgTablespaceFillers[] = {
                    {"oid", [](ui32 index) { return ScalarDatumToPod(ObjectIdGetDatum(index == 1 ? DEFAULTTABLESPACE_OID : GLOBALTABLESPACE_OID)); }},
                    {"spcname", [](ui32 index) { return PointerDatumToPod((Datum)(MakeFixedString(index == 1 ? "pg_default" : "pg_global", NAMEDATALEN))); }},
                };

                ApplyFillers(AllPgTablespaceFillers, Y_ARRAY_SIZE(AllPgTablespaceFillers), PgTablespaceFillers_);
            } else if (Table_ == "pg_shdescription") {
                static const std::pair<const char*, TPgShDescriptionFiller> AllPgShDescriptionFillers[] = {
                    {"objoid", [](ui32 index) { return ScalarDatumToPod(ObjectIdGetDatum(index)); }},
                    {"classoid", [](ui32) { return ScalarDatumToPod(ObjectIdGetDatum(DatabaseRelationId)); }},
                    {"description", [](ui32 index) { return PointerDatumToPod((Datum)MakeVar(
                        index == 1 ? "default template for new databases" :
                        (index == 2 ? "unmodifiable empty database" :
                        "default administrative connection database")
                    )); }},
                };

                ApplyFillers(AllPgShDescriptionFillers, Y_ARRAY_SIZE(AllPgShDescriptionFillers), PgShDescriptionFillers_);
            } else if (Table_ == "pg_stat_gssapi") {
                static const std::pair<const char*, TPgStatGssapiFiller> AllPgStatGssapiFillers[] = {
                    {"encrypted", []() { return ScalarDatumToPod(BoolGetDatum(false)); }},
                    {"gss_authenticated", []() { return ScalarDatumToPod(BoolGetDatum(false)); }},
                    {"pid", []() { return ScalarDatumToPod(Int32GetDatum(GetPID())); }}
                };

                ApplyFillers(AllPgStatGssapiFillers, Y_ARRAY_SIZE(AllPgStatGssapiFillers), PgStatGssapiFillers_);
            } else if (Table_ == "pg_namespace") {
                static const std::pair<const char*, TPgNamespaceFiller> AllPgNamespaceFillers[] = {
                    {"nspname", [](const NPg::TNamespaceDesc& desc) {return PointerDatumToPod((Datum)MakeFixedString(desc.Name, NAMEDATALEN));}},
                    {"oid", [](const NPg::TNamespaceDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(desc.Oid)); }},
                };

                ApplyFillers(AllPgNamespaceFillers, Y_ARRAY_SIZE(AllPgNamespaceFillers), PgNamespaceFillers_);
            } else if (Table_ == "pg_am") {
                static const std::pair<const char*, TPgAmFiller> AllPgAmFillers[] = {
                    {"oid", [](const NPg::TAmDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(desc.Oid)); }},
                    {"amname", [](const NPg::TAmDesc& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.AmName, NAMEDATALEN)); }},
                    {"amtype", [](const NPg::TAmDesc& desc) { return ScalarDatumToPod(CharGetDatum(desc.AmType)); }},
                };

                ApplyFillers(AllPgAmFillers, Y_ARRAY_SIZE(AllPgAmFillers), PgAmFillers_);
            } else if (Table_ == "pg_description") {
                static const std::pair<const char*, TPgDescriptionFiller> AllPgDescriptionFillers[] = {
                    {"objoid", [](const TDescriptionDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(desc.Objoid)); }},
                    {"classoid", [](const TDescriptionDesc& desc) { return ScalarDatumToPod(ObjectIdGetDatum(desc.Classoid)); }},
                    {"objsubid", [](const TDescriptionDesc& desc) { return ScalarDatumToPod(Int32GetDatum(desc.Objsubid)); }},
                    {"description", [](const TDescriptionDesc& desc) { return PointerDatumToPod((Datum)MakeVar(desc.Description)); }}
                };

                ApplyFillers(AllPgDescriptionFillers, Y_ARRAY_SIZE(AllPgDescriptionFillers), PgDescriptionFillers_);
            } else if (Table_ == "pg_tables") {
                static const std::pair<const char*, TTablesFiller> AllPgTablesFillers[] = {
                    {"schemaname", [](const NPg::TTableInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.Schema, NAMEDATALEN)); }},
                    {"tablename", [](const NPg::TTableInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.Name, NAMEDATALEN)); }},
                };

                ApplyFillers(AllPgTablesFillers, Y_ARRAY_SIZE(AllPgTablesFillers), PgTablesFillers_);
            } else if (Table_ == "pg_roles") {
                static const std::pair<const char*, TPgRolesFiller> AllPgRolesFillers[] = {
                    {"rolname", []() { return PointerDatumToPod((Datum)MakeFixedString("postgres", NAMEDATALEN)); }},
                    {"oid", []() { return ScalarDatumToPod(ObjectIdGetDatum(1)); }},
                    {"rolbypassrls", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolsuper", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolinherit", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolcreaterole", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolcreatedb", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolcanlogin", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolreplication", []() { return ScalarDatumToPod(BoolGetDatum(true)); }},
                    {"rolconnlimit", []() { return ScalarDatumToPod(Int32GetDatum(-1)); }},
                    {"rolvaliduntil", []() { return NUdf::TUnboxedValuePod(); }},
                    {"rolconfig", []() { return PointerDatumToPod(MakeArrayOfText({
                        "search_path=public",
                        "default_transaction_isolation=serializable",
                        "standard_conforming_strings=on",
                    })); }},
                };

                ApplyFillers(AllPgRolesFillers, Y_ARRAY_SIZE(AllPgRolesFillers), PgRolesFillers_);
            } else if (Table_ == "pg_stat_database") {
                static const std::pair<const char*, TPgDatabaseStatFiller> AllPgDatabaseStatFillers[] = {
                    {"datid", [](ui32 index) { return ScalarDatumToPod(ObjectIdGetDatum(index ? 3 : 0)); }},
                    {"blks_hit", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"blks_read", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"tup_deleted", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"tup_fetched", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"tup_inserted", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"tup_returned", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"tup_updated", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"xact_commit", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                    {"xact_rollback", [](ui32) { return ScalarDatumToPod(Int64GetDatum(0)); }},
                };

                ApplyFillers(AllPgDatabaseStatFillers, Y_ARRAY_SIZE(AllPgDatabaseStatFillers), PgDatabaseStatFillers_);
            } else if (Table_ == "pg_class") {
                static const std::pair<const char*, TPgClassFiller> AllPgClassFillers[] = {
                    {"oid", [](const NPg::TTableInfo& desc, ui32) { return ScalarDatumToPod(ObjectIdGetDatum(desc.Oid)); }},
                    {"relispartition", [](const NPg::TTableInfo&, ui32) { return ScalarDatumToPod(BoolGetDatum(false)); }},
                    {"relkind", [](const NPg::TTableInfo& desc, ui32) { return ScalarDatumToPod(CharGetDatum(desc.Kind)); }},
                    {"relname", [](const NPg::TTableInfo& desc, ui32) { return PointerDatumToPod((Datum)MakeFixedString(desc.Name, NAMEDATALEN)); }},
                    {"relnamespace", [](const NPg::TTableInfo&, ui32 namespaceOid) { return ScalarDatumToPod(ObjectIdGetDatum(namespaceOid)); }},
                    {"relowner", [](const NPg::TTableInfo&, ui32) { return ScalarDatumToPod(ObjectIdGetDatum(1)); }},
                };

                ApplyFillers(AllPgClassFillers, Y_ARRAY_SIZE(AllPgClassFillers), PgClassFillers_);
            }
        } else {
            if (Table_ == "tables") {
                static const std::pair<const char*, TTablesFiller> AllTablesFillers[] = {
                    {"table_schema", [](const NPg::TTableInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.Schema, NAMEDATALEN)); }},
                    {"table_name", [](const NPg::TTableInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.Name, NAMEDATALEN)); }},
                };

                ApplyFillers(AllTablesFillers, Y_ARRAY_SIZE(AllTablesFillers), TablesFillers_);
            } else if (Table_ == "columns") {
                static const std::pair<const char*, TColumnsFiller> AllColumnsFillers[] = {
                    {"table_schema", [](const NPg::TColumnInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.Schema, NAMEDATALEN)); }},
                    {"table_name", [](const NPg::TColumnInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.TableName, NAMEDATALEN)); }},
                    {"column_name", [](const NPg::TColumnInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.Name, NAMEDATALEN)); }},
                    {"udt_name", [](const NPg::TColumnInfo& desc) { return PointerDatumToPod((Datum)MakeFixedString(desc.UdtType, NAMEDATALEN)); }},
                };

                ApplyFillers(AllColumnsFillers, Y_ARRAY_SIZE(AllColumnsFillers), ColumnsFillers_);
            }
        }
    }

    template <typename T, typename F>
    void ApplyFillers(const T* allFillers, size_t n, TVector<F>& fillers) {
        fillers.resize(ItemType_->GetMembersCount());
        for (size_t i = 0; i < n; ++i) {
            const auto& [name, func] = allFillers[i];
            if (auto pos = ItemType_->FindMemberIndex(name)) {
                fillers[*pos] = func;
            }
        }

        for (const auto& f : fillers) {
            Y_ENSURE(f);
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        TUnboxedValueVector rows;
        if (Cluster_ == "pg_catalog") {
            if (Table_ == "pg_type") {
                NPg::EnumTypes([&](ui32 oid, const NPg::TTypeDesc& desc) {
                    if (desc.ArrayTypeId == desc.TypeId) {
                        return;
                    }

                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgTypeFillers_.size(), items);
                    for (ui32 i = 0; i < PgTypeFillers_.size(); ++i) {
                        items[i] = PgTypeFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });
            } else if (Table_ == "pg_database") {
                for (ui32 index = 1; index <= 3; ++index) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDatabaseFillers_.size(), items);
                    for (ui32 i = 0; i < PgDatabaseFillers_.size(); ++i) {
                        items[i] = PgDatabaseFillers_[i](index);
                    }

                    rows.emplace_back(row);
                }
            } else if (Table_ == "pg_tablespace") {
                for (ui32 index = 1; index <= 2; ++index) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgTablespaceFillers_.size(), items);
                    for (ui32 i = 0; i < PgTablespaceFillers_.size(); ++i) {
                        items[i] = PgTablespaceFillers_[i](index);
                    }

                    rows.emplace_back(row);
                }
            } else if (Table_ == "pg_shdescription") {
                for (ui32 index = 1; index <= 3; ++index) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgShDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgShDescriptionFillers_.size(); ++i) {
                        items[i] = PgShDescriptionFillers_[i](index);
                    }

                    rows.emplace_back(row);
                }
            } else if (Table_ == "pg_stat_gssapi") {
                NUdf::TUnboxedValue* items;
                auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgStatGssapiFillers_.size(), items);
                for (ui32 i = 0; i < PgStatGssapiFillers_.size(); ++i) {
                    items[i] = PgStatGssapiFillers_[i]();
                }

                rows.emplace_back(row);
            } else if (Table_ == "pg_namespace") {
                NPg::EnumNamespace([&](ui32 oid, const NPg::TNamespaceDesc& desc) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(ItemType_->GetMembersCount(), items);
                    for (ui32 i = 0; i < PgNamespaceFillers_.size(); ++i) {
                        items[i] = PgNamespaceFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });
            } else if (Table_ == "pg_am") {
                NPg::EnumAm([&](ui32 oid, const NPg::TAmDesc& desc) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(ItemType_->GetMembersCount(), items);
                    for (ui32 i = 0; i < ItemType_->GetMembersCount(); ++i) {
                        items[i] = PgAmFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });
            } else if (Table_ == "pg_description") {
                TDescriptionDesc desc;
                desc.Classoid = AccessMethodRelationId;
                NPg::EnumAm([&](ui32 oid, const NPg::TAmDesc& desc_) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgDescriptionFillers_.size(); ++i) {
                        desc.Objoid = oid;
                        desc.Description = desc_.Descr;
                        items[i] = PgDescriptionFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });

                desc.Classoid = TypeRelationId;
                NPg::EnumTypes([&](ui32 oid, const NPg::TTypeDesc& desc_) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgDescriptionFillers_.size(); ++i) {
                        desc.Objoid = oid;
                        desc.Description = desc_.Descr;
                        items[i] = PgDescriptionFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });

                desc.Classoid = NamespaceRelationId;
                NPg::EnumNamespace([&](ui32 oid, const NPg::TNamespaceDesc& desc_) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgDescriptionFillers_.size(); ++i) {
                        desc.Objoid = oid;
                        desc.Description = desc_.Descr;
                        items[i] = PgDescriptionFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });

                desc.Classoid = ConversionRelationId;

                NPg::EnumConversions([&](const NPg::TConversionDesc& desc_) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgDescriptionFillers_.size(); ++i) {
                        desc.Objoid = desc_.ConversionId;
                        desc.Description = desc_.Descr;
                        items[i] = PgDescriptionFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });

                desc.Classoid = OperatorRelationId;

                NPg::EnumOperators([&](const NPg::TOperDesc& desc_) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgDescriptionFillers_.size(); ++i) {
                        desc.Objoid = desc_.OperId;
                        desc.Description = desc_.Descr;
                        items[i] = PgDescriptionFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });

                desc.Classoid = ProcedureRelationId;

                NPg::EnumProc([&](ui32, const NPg::TProcDesc& desc_) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDescriptionFillers_.size(), items);
                    for (ui32 i = 0; i < PgDescriptionFillers_.size(); ++i) {
                        desc.Objoid = desc_.ProcId;
                        desc.Description = desc_.Descr;
                        items[i] = PgDescriptionFillers_[i](desc);
                    }

                    rows.emplace_back(row);
                });
            } else if (Table_ == "pg_tables") {
                const auto& tables = NPg::GetStaticTables();
                for (const auto& t : tables) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgTablesFillers_.size(), items);
                    for (ui32 i = 0; i < PgTablesFillers_.size(); ++i) {
                        items[i] = PgTablesFillers_[i](t);
                    }

                    rows.emplace_back(row);
                }
            } else if (Table_ == "pg_roles") {
                NUdf::TUnboxedValue* items;
                auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgRolesFillers_.size(), items);
                for (ui32 i = 0; i < PgRolesFillers_.size(); ++i) {
                    items[i] = PgRolesFillers_[i]();
                }

                rows.emplace_back(row);
            } else if (Table_ == "pg_stat_database") {
                for (ui32 index = 0; index <= 1; ++index) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgDatabaseStatFillers_.size(), items);
                    for (ui32 i = 0; i < PgDatabaseStatFillers_.size(); ++i) {
                        items[i] = PgDatabaseStatFillers_[i](index);
                    }

                    rows.emplace_back(row);
                }
            } else if (Table_ == "pg_class") {
                const auto& tables = NPg::GetStaticTables();
                THashMap<TString, ui32> namespaces;
                NPg::EnumNamespace([&](ui32 oid, const NPg::TNamespaceDesc& desc) {
                    namespaces[desc.Name] = oid;
                });

                for (const auto& t : tables) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(PgClassFillers_.size(), items);
                    for (ui32 i = 0; i < PgClassFillers_.size(); ++i) {
                        items[i] = PgClassFillers_[i](t, namespaces[t.Schema]);
                    }

                    rows.emplace_back(row);
                }
            }
        } else {
            if (Table_ == "tables") {
                const auto& tables = NPg::GetStaticTables();
                for (const auto& t : tables) {
                    NUdf::TUnboxedValue* items;
                    auto row = compCtx.HolderFactory.CreateDirectArrayHolder(TablesFillers_.size(), items);
                    for (ui32 i = 0; i < TablesFillers_.size(); ++i) {
                        items[i] = TablesFillers_[i](t);
                    }

                    rows.emplace_back(row);
                }
            } else if (Table_ == "columns") {
                const auto& columns = NPg::GetStaticColumns();
                for (const auto& t : columns) {
                    for (const auto& c : t.second) {
                        NUdf::TUnboxedValue* items;
                        auto row = compCtx.HolderFactory.CreateDirectArrayHolder(ColumnsFillers_.size(), items);
                        for (ui32 i = 0; i < ColumnsFillers_.size(); ++i) {
                            items[i] = ColumnsFillers_[i](c);
                        }

                        rows.emplace_back(row);
                    }
                }
            }
        }

        return compCtx.HolderFactory.VectorAsVectorHolder(std::move(rows));
    }

private:
    void RegisterDependencies() const final {
    }

    const std::string_view Cluster_;
    const std::string_view Table_;
    TStructType* const ItemType_;

    using TPgTypeFiller = NUdf::TUnboxedValuePod(*)(const NPg::TTypeDesc& desc);
    TVector<TPgTypeFiller> PgTypeFillers_;
    using TPgDatabaseFiller = NUdf::TUnboxedValuePod(*)(ui32 index);
    TVector<TPgDatabaseFiller> PgDatabaseFillers_;
    using TPgTablespaceFiller = NUdf::TUnboxedValuePod(*)(ui32 index);
    TVector<TPgTablespaceFiller> PgTablespaceFillers_;
    using TPgShDescriptionFiller = NUdf::TUnboxedValuePod(*)(ui32 index);
    TVector<TPgShDescriptionFiller> PgShDescriptionFillers_;
    using TPgStatGssapiFiller = NUdf::TUnboxedValuePod(*)();
    TVector<TPgStatGssapiFiller> PgStatGssapiFillers_;
    using TPgNamespaceFiller = NUdf::TUnboxedValuePod(*)(const NPg::TNamespaceDesc&);
    TVector<TPgNamespaceFiller> PgNamespaceFillers_;
    using TPgAmFiller = NUdf::TUnboxedValuePod(*)(const NPg::TAmDesc&);
    TVector<TPgAmFiller> PgAmFillers_;
    using TPgRolesFiller = NUdf::TUnboxedValuePod(*)();
    TVector<TPgRolesFiller> PgRolesFillers_;
    using TPgDatabaseStatFiller = NUdf::TUnboxedValuePod(*)(ui32 index);
    TVector<TPgDatabaseStatFiller> PgDatabaseStatFillers_;

    struct TDescriptionDesc {
        ui32 Objoid = 0;
        ui32 Classoid = 0;
        i32 Objsubid = 0;
        TString Description;
    };

    using TPgDescriptionFiller = NUdf::TUnboxedValuePod(*)(const TDescriptionDesc&);
    TVector<TPgDescriptionFiller> PgDescriptionFillers_;

    using TTablesFiller = NUdf::TUnboxedValuePod(*)(const NPg::TTableInfo&);
    TVector<TTablesFiller> PgTablesFillers_;
    TVector<TTablesFiller> TablesFillers_;

    using TColumnsFiller = NUdf::TUnboxedValuePod(*)(const NPg::TColumnInfo&);
    TVector<TColumnsFiller> ColumnsFillers_;

    using TPgClassFiller = NUdf::TUnboxedValuePod(*)(const NPg::TTableInfo&, ui32 namespaceOid);
    TVector<TPgClassFiller> PgClassFillers_;
};

class TFunctionCallInfo {
public:
    TFunctionCallInfo(ui32 numArgs, const FmgrInfo* finfo)
        : NumArgs(numArgs)
        , CopyFmgrInfo(*finfo)
    {
        if (!finfo->fn_addr) {
            return;
        }

        MemSize = SizeForFunctionCallInfo(numArgs);
        Ptr = TWithDefaultMiniKQLAlloc::AllocWithSize(MemSize);
        auto& callInfo = Ref();
        Zero(callInfo);
        callInfo.flinfo = &CopyFmgrInfo; // client may mutate fn_extra
        callInfo.nargs = NumArgs;
        callInfo.fncollation = DEFAULT_COLLATION_OID;
    }

    FunctionCallInfoBaseData& Ref() {
        Y_ENSURE(Ptr);
        return *(FunctionCallInfoBaseData*)Ptr;
    }

    ~TFunctionCallInfo() {
        if (Ptr) {
            TWithDefaultMiniKQLAlloc::FreeWithSize(Ptr, MemSize);
        }
    }

    TFunctionCallInfo(const TFunctionCallInfo&) = delete;
    void operator=(const TFunctionCallInfo&) = delete;

private:
    const ui32 NumArgs = 0;
    ui32 MemSize = 0;
    void* Ptr = nullptr;
    FmgrInfo CopyFmgrInfo;
};

class TReturnSetInfo {
public:
    TReturnSetInfo() {
        Ptr = TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(ReturnSetInfo));
        Zero(Ref());
        Ref().type = T_ReturnSetInfo;
    }

    ~TReturnSetInfo() {
        Free();
    }

    void Free() {
        if (!Ptr) {
            return;
        }

        if (Ref().expectedDesc) {
            FreeTupleDesc(Ref().expectedDesc);
        }

        if (Ref().setResult) {
            tuplestore_end(Ref().setResult);
        }

        TWithDefaultMiniKQLAlloc::FreeWithSize(Ptr, sizeof(ReturnSetInfo));
        Ptr = nullptr;
    }

    ReturnSetInfo& Ref() {
        Y_ENSURE(Ptr, "ReturnSetInfo is dead");

        return *static_cast<ReturnSetInfo*>(Ptr);
    }

private:
    void* Ptr = nullptr;
};

class TExprContextHolder {
public:
    TExprContextHolder() {
        Ptr = CreateStandaloneExprContext();
    }

    ExprContext& Ref() {
        Y_ENSURE(Ptr, "TExprContextHolder is dead");

        return *Ptr;
    }

    ~TExprContextHolder() {
        Free();
    }

    void Free() {
        if (!Ptr) {
            return;
        }
        FreeExprContext(Ptr, true);
        Ptr = nullptr;
    }

private:
    ExprContext* Ptr;
};


template <typename TDerived>
class TPgResolvedCallBase : public TMutableComputationNode<TDerived> {
    typedef TMutableComputationNode<TDerived> TBaseComputation;
public:
    TPgResolvedCallBase(TComputationMutables& mutables, const std::string_view& name, ui32 id,
        TComputationNodePtrVector&& argNodes, TVector<TType*>&& argTypes, bool isList, const TStructType* structType)
        : TBaseComputation(mutables)
        , Name(name)
        , Id(id)
        , ProcDesc(NPg::LookupProc(id))
        , RetTypeDesc(NPg::LookupType(ProcDesc.ResultType))
        , ArgNodes(std::move(argNodes))
        , ArgTypes(std::move(argTypes))
        , StructType(structType)
        , PgFuncArgsList(nullptr, &free)
    {
        Zero(FInfo);
        Y_ENSURE(Id);
        fmgr_info(Id, &FInfo);
        Y_ENSURE(FInfo.fn_retset == isList);
        Y_ENSURE(FInfo.fn_addr);
        Y_ENSURE(FInfo.fn_nargs == ArgNodes.size());
        ArgDesc.reserve(ProcDesc.ArgTypes.size());
        for (ui32 i = 0; i < ProcDesc.ArgTypes.size(); ++i) {
            ui32 type;
            // extract real type from input args
            auto argType = ArgTypes[i];
            if (argType->IsPg()) {
                type = static_cast<TPgType*>(argType)->GetTypeId();
            } else {
                // keep original description for nulls
                type = ProcDesc.ArgTypes[i];
            }

            ArgDesc.emplace_back(NPg::LookupType(type));
        }

        Y_ENSURE(ArgDesc.size() == ArgNodes.size());
        Zero(PgFuncNode);
        PgArgNodes.resize(ArgDesc.size());
        for (size_t i = 0; i < ArgDesc.size(); ++i) {
            auto& v = PgArgNodes[i];
            Zero(v);
            v.xpr.type = T_Var;
            v.vartype = ArgDesc[i].TypeId;
            v.vartypmod = -1;
        }

        PgFuncArgsList.reset((List*)malloc(offsetof(List, initial_elements) + ArgDesc.size() * sizeof(ListCell)));
        PgFuncArgsList->type = T_List;
        PgFuncArgsList->elements = PgFuncArgsList->initial_elements;
        PgFuncArgsList->length = PgFuncArgsList->max_length = ArgDesc.size();
        for (size_t i = 0; i < ArgDesc.size(); ++i) {
            PgFuncArgsList->elements[i].ptr_value = &PgArgNodes[i];
        }

        PgFuncNode.xpr.type = T_FuncExpr;
        PgFuncNode.funcid = ProcDesc.ProcId;
        PgFuncNode.funcresulttype = ProcDesc.ResultType;
        PgFuncNode.funcretset = ProcDesc.ReturnSet;
        PgFuncNode.args = PgFuncArgsList.get();
        FInfo.fn_expr = (Node*)&PgFuncNode;
    }

private:
    void RegisterDependencies() const final {
        for (const auto node : ArgNodes) {
            this->DependsOn(node);
        }
    }

protected:
    const std::string_view Name;
    const ui32 Id;
    FmgrInfo FInfo;
    const NPg::TProcDesc ProcDesc;
    const NPg::TTypeDesc RetTypeDesc;
    const TComputationNodePtrVector ArgNodes;
    const TVector<TType*> ArgTypes;
    const TStructType* StructType;
    TVector<NPg::TTypeDesc> ArgDesc;
    
    TVector<Var> PgArgNodes;
    std::unique_ptr<List, decltype(&free)> PgFuncArgsList;
    FuncExpr PgFuncNode;
};

struct TPgResolvedCallState : public TComputationValue<TPgResolvedCallState> {
    TPgResolvedCallState(TMemoryUsageInfo* memInfo, ui32 numArgs, const FmgrInfo* finfo)
        : TComputationValue(memInfo)
        , CallInfo(numArgs, finfo)
        , Args(numArgs)
    {
    }

    TFunctionCallInfo CallInfo;
    TUnboxedValueVector Args;
};

template <bool UseContext>
class TPgResolvedCall : public TPgResolvedCallBase<TPgResolvedCall<UseContext>> {
    typedef TPgResolvedCallBase<TPgResolvedCall<UseContext>> TBaseComputation;
public:
    TPgResolvedCall(TComputationMutables& mutables, const std::string_view& name, ui32 id,
        TComputationNodePtrVector&& argNodes, TVector<TType*>&& argTypes)
        : TBaseComputation(mutables, name, id, std::move(argNodes), std::move(argTypes), false, nullptr)
        , StateIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto& state = this->GetState(compCtx);
        auto& callInfo = state.CallInfo.Ref();
        auto& args = state.Args;
        if constexpr (UseContext) {
            callInfo.context = (Node*)TlsAllocState->CurrentContext;
        }

        callInfo.isnull = false;
        for (ui32 i = 0; i < this->ArgNodes.size(); ++i) {
            args[i] = std::move(this->ArgNodes[i]->GetValue(compCtx));
            auto& value = args[i];
            NullableDatum argDatum = { 0, false };
            if (!value) {
                if (this->FInfo.fn_strict) {
                    return NUdf::TUnboxedValuePod();
                }

                argDatum.isnull = true;
            } else {
                argDatum.value = this->ArgDesc[i].PassByValue ?
                    ScalarDatumFromPod(value) :
                    PointerDatumFromPod(value);
            }

            callInfo.args[i] = argDatum;
        }

        if constexpr (!UseContext) {
            TPAllocScope call;
            return this->DoCall(callInfo);
        }

        if constexpr (UseContext) {
            return this->DoCall(callInfo);
        }
    }

private:
    NUdf::TUnboxedValuePod DoCall(FunctionCallInfoBaseData& callInfo) const {
        auto ret = this->FInfo.fn_addr(&callInfo);
        if (callInfo.isnull) {
            return NUdf::TUnboxedValuePod();
        }

        return AnyDatumToPod(ret, this->RetTypeDesc.PassByValue);
    }

    TPgResolvedCallState& GetState(TComputationContext& compCtx) const {
        auto& result = compCtx.MutableValues[this->StateIndex];
        if (!result.HasValue()) {
            result = compCtx.HolderFactory.Create<TPgResolvedCallState>(this->ArgNodes.size(), &this->FInfo);
        }

        return *static_cast<TPgResolvedCallState*>(result.AsBoxed().Get());
    }

    const ui32 StateIndex;
};

class TPgResolvedMultiCall : public TPgResolvedCallBase<TPgResolvedMultiCall> {
    typedef TPgResolvedCallBase<TPgResolvedMultiCall> TBaseComputation;
private:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, const std::string_view& name, const TUnboxedValueVector& args,
                const TVector<NPg::TTypeDesc>& argDesc, const NPg::TTypeDesc& retTypeDesc, const NPg::TProcDesc& procDesc,
                const FmgrInfo* fInfo, const TStructType* structType, const THolderFactory& holderFactory)
                : TComputationValue<TIterator>(memInfo)
                , Name(name)
                , Args(args)
                , ArgDesc(argDesc)
                , RetTypeDesc(retTypeDesc)
                , ProcDesc(procDesc)
                , CallInfo(argDesc.size(), fInfo)
                , StructType(structType)
                , HolderFactory(holderFactory)
            {
                auto& callInfo = CallInfo.Ref();
                callInfo.resultinfo = (fmNodePtr)&RSInfo.Ref();
                auto& rsInfo = *(ReturnSetInfo*)callInfo.resultinfo;
                rsInfo.econtext = &ExprContextHolder.Ref();
                rsInfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
                rsInfo.returnMode = SFRM_ValuePerCall;
                rsInfo.setResult = nullptr;
                rsInfo.setDesc = nullptr;
                if (RetTypeDesc.TypeId != RECORDOID) {
                    rsInfo.expectedDesc = CreateTemplateTupleDesc(1);
                    TupleDescInitEntry(rsInfo.expectedDesc, (AttrNumber) 1, nullptr, RetTypeDesc.TypeId, -1, 0);
                } else {
                    if (StructType) {
                        YQL_ENSURE(ProcDesc.OutputArgNames.size() == ProcDesc.OutputArgTypes.size());
                        YQL_ENSURE(ProcDesc.OutputArgNames.size() == StructType->GetMembersCount());
                        StructIndicies.resize(StructType->GetMembersCount());
                    }

                    rsInfo.expectedDesc = CreateTemplateTupleDesc(ProcDesc.OutputArgTypes.size());
                    for (size_t i = 0; i < ProcDesc.OutputArgTypes.size(); ++i) {
                        auto attrName = ProcDesc.OutputArgNames.empty() ? nullptr : ProcDesc.OutputArgNames[i].c_str();
                        TupleDescInitEntry(rsInfo.expectedDesc, (AttrNumber) 1 + i, attrName, ProcDesc.OutputArgTypes[i], -1, 0);
                        if (StructType) {
                            auto index = StructType->FindMemberIndex(ProcDesc.OutputArgNames[i]);
                            YQL_ENSURE(index);
                            StructIndicies[i] = *index;
                        }
                    }

                    rsInfo.expectedDesc = BlessTupleDesc(rsInfo.expectedDesc);
                }
                
                TupleSlot = MakeSingleTupleTableSlot(rsInfo.expectedDesc, &TTSOpsMinimalTuple);
                for (ui32 i = 0; i < args.size(); ++i) {
                    const auto& value = args[i];
                    NullableDatum argDatum = { 0, false };
                    if (!value) {
                        argDatum.isnull = true;
                    } else {
                        argDatum.value = ArgDesc[i].PassByValue ?
                            ScalarDatumFromPod(value) :
                            PointerDatumFromPod(value);
                    }

                    callInfo.args[i] = argDatum;
                }
            }

            ~TIterator() {
                FinishAndFree();
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (IsFinished) {
                    return false;
                }

                if (RSInfo.Ref().setResult) {
                    return CopyTuple(value);
                }

                auto& callInfo = CallInfo.Ref();
                callInfo.isnull = false;
                auto ret = callInfo.flinfo->fn_addr(&callInfo);
                if (RSInfo.Ref().returnMode == SFRM_Materialize) {
                    Y_ENSURE(RSInfo.Ref().isDone == ExprSingleResult);
                    Y_ENSURE(RSInfo.Ref().setResult);
                    auto readPtr = tuplestore_alloc_read_pointer(RSInfo.Ref().setResult, 0);
                    tuplestore_select_read_pointer(RSInfo.Ref().setResult, readPtr);
                    return CopyTuple(value);
                } else {
                    YQL_ENSURE(!StructType);
                    if (RSInfo.Ref().isDone == ExprEndResult) {
                        FinishAndFree();
                        return false;
                    }

                    if (callInfo.isnull) {
                        value = NUdf::TUnboxedValuePod();
                    } else {
                        value = AnyDatumToPod(ret, RetTypeDesc.PassByValue);
                    }

                    return true;
                }
            }

            bool CopyTuple(NUdf::TUnboxedValue& value) {
                if (!tuplestore_gettupleslot(RSInfo.Ref().setResult, true, false, TupleSlot)) {
                    FinishAndFree();
                    return false;
                }
                
                slot_getallattrs(TupleSlot);
                if (RetTypeDesc.TypeId == RECORDOID) {
                    if (StructType) {
                        Y_ENSURE(TupleSlot->tts_nvalid == StructType->GetMembersCount());
                        NUdf::TUnboxedValue* itemsPtr;
                        value = HolderFactory.CreateDirectArrayHolder(StructType->GetMembersCount(), itemsPtr);
                        for (ui32 i = 0; i < StructType->GetMembersCount(); ++i) {
                            itemsPtr[StructIndicies[i]] = CloneTupleItem(i);
                        }
                    } else {
                        // whole record value
                        auto tupleDesc = RSInfo.Ref().expectedDesc;
                        auto tuple = ExecCopySlotHeapTuple(TupleSlot);
                        auto result = (HeapTupleHeader) palloc(tuple->t_len);
                        memcpy(result, tuple->t_data, tuple->t_len);
                        HeapTupleHeaderSetDatumLength(result, tuple->t_len);
                        HeapTupleHeaderSetTypeId(result, tupleDesc->tdtypeid);
                        HeapTupleHeaderSetTypMod(result, tupleDesc->tdtypmod);
                        heap_freetuple(tuple);
                        value = PointerDatumToPod(HeapTupleHeaderGetDatum(result));
                    }
                } else {
                    Y_ENSURE(TupleSlot->tts_nvalid == 1);
                    value = CloneTupleItem(0);
                }

                return true;
            }

            NUdf::TUnboxedValuePod CloneTupleItem(ui32 index) {
                if (TupleSlot->tts_isnull[index]) {
                    return NUdf::TUnboxedValuePod();
                } else {
                    auto datum = TupleSlot->tts_values[index];
                    if (RetTypeDesc.PassByValue) {
                        return ScalarDatumToPod(datum);
                    } else if (RetTypeDesc.TypeLen == -1) {
                        const text* orig = (const text*)datum;
                        return PointerDatumToPod((Datum)MakeVar(GetVarBuf(orig)));
                    } else if(RetTypeDesc.TypeLen == -2) {
                        const char* orig = (const char*)datum;
                        return PointerDatumToPod((Datum)MakeCString(orig));
                    } else {
                        const char* orig = (const char*)datum;
                        return PointerDatumToPod((Datum)MakeFixedString(orig, RetTypeDesc.TypeLen));
                    }
                }
            }

            void FinishAndFree() {
                if (TupleSlot) {
                    ExecDropSingleTupleTableSlot(TupleSlot);
                    TupleSlot = nullptr;
                }
                RSInfo.Free();
                ExprContextHolder.Free();

                IsFinished = true;
            }

            const std::string_view Name;
            TUnboxedValueVector Args;
            const TVector<NPg::TTypeDesc>& ArgDesc;
            const NPg::TTypeDesc& RetTypeDesc;
            const NPg::TProcDesc& ProcDesc;
            TExprContextHolder ExprContextHolder;
            TFunctionCallInfo CallInfo;
            const TStructType* StructType;
            const THolderFactory& HolderFactory;
            TReturnSetInfo RSInfo;
            bool IsFinished = false;
            TupleTableSlot* TupleSlot = nullptr;
            TVector<ui32> StructIndicies;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx,
            const std::string_view& name, TUnboxedValueVector&& args, const TVector<NPg::TTypeDesc>& argDesc,
            const NPg::TTypeDesc& retTypeDesc, const NPg::TProcDesc& procDesc, const FmgrInfo* fInfo,
            const TStructType* structType, const THolderFactory& holderFactory)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , Name(name)
            , Args(args)
            , ArgDesc(argDesc)
            , RetTypeDesc(retTypeDesc)
            , ProcDesc(procDesc)
            , FInfo(fInfo)
            , StructType(structType)
            , HolderFactory(holderFactory)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(Name, Args, ArgDesc, RetTypeDesc, ProcDesc, FInfo, StructType, CompCtx.HolderFactory);
        }

        TComputationContext& CompCtx;
        const std::string_view Name;
        TUnboxedValueVector Args;
        const TVector<NPg::TTypeDesc>& ArgDesc;
        const NPg::TTypeDesc& RetTypeDesc;
        const NPg::TProcDesc& ProcDesc;
        const FmgrInfo* FInfo;
        const TStructType* StructType;
        const THolderFactory& HolderFactory;
    };

public:
    TPgResolvedMultiCall(TComputationMutables& mutables, const std::string_view& name, ui32 id,
        TComputationNodePtrVector&& argNodes, TVector<TType*>&& argTypes, const TStructType* structType)
        : TBaseComputation(mutables, name, id, std::move(argNodes), std::move(argTypes), true, structType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        TUnboxedValueVector args;
        args.reserve(ArgNodes.size());
        for (ui32 i = 0; i < ArgNodes.size(); ++i) {
            auto value = ArgNodes[i]->GetValue(compCtx);
            args.push_back(value);
        }

        return compCtx.HolderFactory.Create<TListValue>(compCtx, Name, std::move(args), ArgDesc, RetTypeDesc, ProcDesc, &FInfo, StructType, compCtx.HolderFactory);
    }
};

class TPgCast : public TMutableComputationNode<TPgCast> {
    typedef TMutableComputationNode<TPgCast> TBaseComputation;
public:
    TPgCast(TComputationMutables& mutables, ui32 sourceId, ui32 targetId, IComputationNode* arg, IComputationNode* typeMod)
        : TBaseComputation(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , SourceId(sourceId)
        , TargetId(targetId)
        , Arg(arg)
        , TypeMod(typeMod)
        , SourceTypeDesc(SourceId ? NPg::LookupType(SourceId) : NPg::TTypeDesc())
        , TargetTypeDesc(NPg::LookupType(targetId))
        , IsSourceArray(SourceId && SourceTypeDesc.TypeId == SourceTypeDesc.ArrayTypeId)
        , IsTargetArray(TargetTypeDesc.TypeId == TargetTypeDesc.ArrayTypeId)
        , SourceElemDesc(SourceId ? NPg::LookupType(IsSourceArray ? SourceTypeDesc.ElementTypeId : SourceTypeDesc.TypeId) : NPg::TTypeDesc())
        , TargetElemDesc(NPg::LookupType(IsTargetArray ? TargetTypeDesc.ElementTypeId : TargetTypeDesc.TypeId))
    {
        TypeIOParam = MakeTypeIOParam(TargetTypeDesc);

        Zero(FInfo1);
        Zero(FInfo2);
        if (TypeMod && SourceId == TargetId && NPg::HasCast(TargetElemDesc.TypeId, TargetElemDesc.TypeId)) {
            const auto& cast = NPg::LookupCast(TargetElemDesc.TypeId, TargetElemDesc.TypeId);

            Y_ENSURE(cast.FunctionId);
            fmgr_info(cast.FunctionId, &FInfo1);
            Y_ENSURE(!FInfo1.fn_retset);
            Y_ENSURE(FInfo1.fn_addr);
            Y_ENSURE(FInfo1.fn_nargs >= 2 && FInfo1.fn_nargs <= 3);
            ConvertLength = true;
            ArrayCast = IsSourceArray;
            return;
        }

        if (SourceId == 0 || SourceId == TargetId) {
            return;
        }

        ui32 funcId;
        ui32 funcId2 = 0;
        if (!NPg::HasCast(SourceElemDesc.TypeId, TargetElemDesc.TypeId) || (IsSourceArray != IsTargetArray)) {
            ArrayCast = IsSourceArray && IsTargetArray;
            if (IsSourceArray && !IsTargetArray) {
                Y_ENSURE(TargetTypeDesc.Category == 'S' || TargetId == UNKNOWNOID);
                funcId = NPg::LookupProc("array_out", { 0 }).ProcId;
            } else if (IsTargetArray && !IsSourceArray) {
                Y_ENSURE(SourceElemDesc.Category == 'S' || SourceId == UNKNOWNOID);
                funcId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
            } else if (SourceElemDesc.Category == 'S' || SourceId == UNKNOWNOID) {
                funcId = TargetElemDesc.InFuncId;
            } else {
                Y_ENSURE(TargetTypeDesc.Category == 'S' || TargetId == UNKNOWNOID);
                funcId = SourceElemDesc.OutFuncId;
            }
        } else {
            Y_ENSURE(IsSourceArray == IsTargetArray);
            ArrayCast = IsSourceArray;

            const auto& cast = NPg::LookupCast(SourceElemDesc.TypeId, TargetElemDesc.TypeId);
            switch (cast.Method) {
                case NPg::ECastMethod::Binary:
                    return;
                case NPg::ECastMethod::Function: {
                    Y_ENSURE(cast.FunctionId);
                    funcId = cast.FunctionId;
                    break;
                }
                case NPg::ECastMethod::InOut: {
                    funcId = SourceElemDesc.OutFuncId;
                    funcId2 = TargetElemDesc.InFuncId;
                    break;
                }
            }
        }

        Y_ENSURE(funcId);
        fmgr_info(funcId, &FInfo1);
        Y_ENSURE(!FInfo1.fn_retset);
        Y_ENSURE(FInfo1.fn_addr);
        Y_ENSURE(FInfo1.fn_nargs >= 1 && FInfo1.fn_nargs <= 3);
        Func1Lookup = NPg::LookupProc(funcId);
        Y_ENSURE(Func1Lookup.ArgTypes.size() >= 1 && Func1Lookup.ArgTypes.size() <= 3);
        if (NPg::LookupType(Func1Lookup.ArgTypes[0]).TypeLen == -2 && SourceElemDesc.Category == 'S') {
            ConvertArgToCString = true;
        }

        if (funcId2) {
            Y_ENSURE(funcId2);
            fmgr_info(funcId2, &FInfo2);
            Y_ENSURE(!FInfo2.fn_retset);
            Y_ENSURE(FInfo2.fn_addr);
            Y_ENSURE(FInfo2.fn_nargs == 1);
            Func2Lookup = NPg::LookupProc(funcId2);
            Y_ENSURE(Func2Lookup.ArgTypes.size() == 1);
        }

        if (!funcId2) {
            if (NPg::LookupType(Func1Lookup.ResultType).TypeLen == -2 && TargetElemDesc.Category == 'S') {
                ConvertResFromCString = true;
            }
        } else {
            const auto& Func2ArgType = NPg::LookupType(Func2Lookup.ArgTypes[0]);
            if (NPg::LookupType(Func1Lookup.ResultType).TypeLen == -2 && Func2ArgType.Category == 'S') {
                ConvertResFromCString = true;
            }

            if (NPg::LookupType(Func2Lookup.ResultType).TypeLen == -2 && TargetElemDesc.Category == 'S') {
                ConvertResFromCString2 = true;
            }
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value) {
            return value.Release();
        }

        i32 typeMod = -1;
        if (TypeMod) {
            typeMod = DatumGetInt32(ScalarDatumFromPod(TypeMod->GetValue(compCtx)));
        }

        if (!FInfo1.fn_addr) {
            // binary compatible
            if (!ArrayCast) {
                return value.Release();
            } else {
                // clone array with new target type in the header
                auto datum = PointerDatumFromPod(value);
                ArrayType* arr = DatumGetArrayTypePCopy(datum);
                ARR_ELEMTYPE(arr) = TargetElemDesc.TypeId;
                return PointerDatumToPod(PointerGetDatum(arr));
            }
        }

        TPAllocScope call;
        auto& state = GetState(compCtx);
        if (ArrayCast) {
            auto arr = (ArrayType*)DatumGetPointer(PointerDatumFromPod(value));
            auto ndim = ARR_NDIM(arr);
            auto dims = ARR_DIMS(arr);
            auto lb = ARR_LBOUND(arr);
            auto nitems = ArrayGetNItems(ndim, dims);

            Datum* elems = (Datum*)TWithDefaultMiniKQLAlloc::AllocWithSize(nitems * sizeof(Datum));
            Y_DEFER {
                TWithDefaultMiniKQLAlloc::FreeWithSize(elems, nitems * sizeof(Datum));
            };

            bool* nulls = (bool*)TWithDefaultMiniKQLAlloc::AllocWithSize(nitems);
            Y_DEFER {
                TWithDefaultMiniKQLAlloc::FreeWithSize(nulls, nitems);
            };

            array_iter iter;
            array_iter_setup(&iter, (AnyArrayType*)arr);
            for (ui32 i = 0; i < nitems; ++i) {
                bool isNull;
                auto datum = array_iter_next(&iter, &isNull, i, SourceElemDesc.TypeLen,
                    SourceElemDesc.PassByValue, SourceElemDesc.TypeAlign);
                if (isNull) {
                    nulls[i] = true;
                    continue;
                } else {
                    nulls[i] = false;
                    elems[i] = ConvertDatum(datum, state, typeMod);
                }
            }

            auto ret = construct_md_array(elems, nulls, ndim, dims, lb, TargetElemDesc.TypeId,
                TargetElemDesc.TypeLen, TargetElemDesc.PassByValue, TargetElemDesc.TypeAlign);

            return PointerDatumToPod(PointerGetDatum(ret));
        } else {
            auto datum = SourceTypeDesc.PassByValue ?
                ScalarDatumFromPod(value) :
                PointerDatumFromPod(value);
            auto ret = ConvertDatum(datum, state, typeMod);
            return AnyDatumToPod(ret, TargetTypeDesc.PassByValue);
        }
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg);
        if (TypeMod) {
            DependsOn(TypeMod);
        }
    }

    struct TState : public TComputationValue<TState> {
        TState(TMemoryUsageInfo* memInfo, const FmgrInfo* finfo1, const FmgrInfo* finfo2)
            : TComputationValue(memInfo)
            , CallInfo1(3, finfo1)
            , CallInfo2(1, finfo2)
        {
        }

        TFunctionCallInfo CallInfo1, CallInfo2;
    };

    TState& GetState(TComputationContext& compCtx) const {
        auto& result = compCtx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = compCtx.HolderFactory.Create<TState>(&FInfo1, &FInfo2);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

    Datum ConvertDatum(Datum datum, TState& state, i32 typeMod) const {
        auto& callInfo1 = state.CallInfo1.Ref();
        callInfo1.isnull = false;
        NullableDatum argDatum = { datum, false };
        void* freeCString = nullptr;
        Y_DEFER {
            if (freeCString) {
                pfree(freeCString);
            }
        };

        if (ConvertArgToCString) {
            argDatum.value = (Datum)MakeCString(GetVarBuf((const text*)argDatum.value));
            freeCString = (void*)argDatum.value;
        }

        callInfo1.args[0] = argDatum;
        if (ConvertLength) {
            callInfo1.args[1] = { Int32GetDatum(typeMod), false };
            callInfo1.args[2] = { BoolGetDatum(true), false };
        } else {
            if (FInfo1.fn_nargs == 2) {
                callInfo1.args[1] = { Int32GetDatum(typeMod), false };
            } else {
                callInfo1.args[1] = { ObjectIdGetDatum(TypeIOParam), false };
                callInfo1.args[2] = { Int32GetDatum(typeMod), false };
            }
        }

        void* freeMem = nullptr;
        void* freeMem2 = nullptr;
        Y_DEFER {
            if (freeMem) {
                pfree(freeMem);
            }

            if (freeMem2) {
                pfree(freeMem2);
            }
        };

        {
            auto ret = FInfo1.fn_addr(&callInfo1);
            Y_ENSURE(!callInfo1.isnull);

            if (ConvertResFromCString) {
                freeMem = (void*)ret;
                ret = (Datum)MakeVar((const char*)ret);
            }

            if (FInfo2.fn_addr) {
                auto& callInfo2 = state.CallInfo1.Ref();
                callInfo2.isnull = false;
                NullableDatum argDatum2 = { ret, false };
                callInfo2.args[0] = argDatum2;

                auto ret2 = FInfo2.fn_addr(&callInfo2);
                pfree((void*)ret);

                Y_ENSURE(!callInfo2.isnull);
                ret = ret2;
            }

            if (ConvertResFromCString2) {
                freeMem2 = (void*)ret;
                ret = (Datum)MakeVar((const char*)ret);
            }

            return ret;
        }
    }

    const ui32 StateIndex;
    const ui32 SourceId;
    const ui32 TargetId;
    IComputationNode* const Arg;
    IComputationNode* const TypeMod;
    const NPg::TTypeDesc SourceTypeDesc;
    const NPg::TTypeDesc TargetTypeDesc;
    const bool IsSourceArray;
    const bool IsTargetArray;
    const NPg::TTypeDesc SourceElemDesc;
    const NPg::TTypeDesc TargetElemDesc;
    FmgrInfo FInfo1, FInfo2;
    NPg::TProcDesc Func1Lookup, Func2Lookup;
    bool ConvertArgToCString = false;
    bool ConvertResFromCString = false;
    bool ConvertResFromCString2 = false;
    ui32 TypeIOParam = 0;
    bool ArrayCast = false;
    bool ConvertLength = false;
};


template <NUdf::EDataSlot Slot>
NUdf::TUnboxedValuePod ConvertToPgValue(NUdf::TUnboxedValuePod value, TMaybe<NUdf::EDataSlot> actualSlot = {}) {
#ifndef NDEBUG
    // todo: improve checks
    if (actualSlot && Slot != *actualSlot) {
        throw yexception() << "Invalid data slot in ConvertToPgValue, expected " << Slot << ", but actual: " << *actualSlot;
    }
#else
    Y_UNUSED(actualSlot);
#endif

    switch (Slot) {
    case NUdf::EDataSlot::Bool:
        return ScalarDatumToPod(BoolGetDatum(value.Get<bool>()));
    case NUdf::EDataSlot::Int16:
        return ScalarDatumToPod(Int16GetDatum(value.Get<i16>()));
    case NUdf::EDataSlot::Int32:
        return ScalarDatumToPod(Int32GetDatum(value.Get<i32>()));
    case NUdf::EDataSlot::Int64:
        return ScalarDatumToPod(Int64GetDatum(value.Get<i64>()));
    case NUdf::EDataSlot::Float:
        return ScalarDatumToPod(Float4GetDatum(value.Get<float>()));
    case NUdf::EDataSlot::Double:
        return ScalarDatumToPod(Float8GetDatum(value.Get<double>()));
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8: {
        const auto& ref = value.AsStringRef();
        return PointerDatumToPod((Datum)MakeVar(ref));
    }
    default:
        ythrow yexception() << "Unexpected data slot in ConvertToPgValue: " << Slot;
    }
}

template <NUdf::EDataSlot Slot, bool IsCString>
NUdf::TUnboxedValuePod ConvertFromPgValue(NUdf::TUnboxedValuePod value, TMaybe<NUdf::EDataSlot> actualSlot = {}) {
#ifndef NDEBUG
    // todo: improve checks
    if (actualSlot && Slot != *actualSlot) {
        throw yexception() << "Invalid data slot in ConvertFromPgValue, expected " << Slot << ", but actual: " << *actualSlot;
    }
#else
    Y_UNUSED(actualSlot);
#endif

    switch (Slot) {
    case NUdf::EDataSlot::Bool:
        return NUdf::TUnboxedValuePod((bool)DatumGetBool(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Int16:
        return NUdf::TUnboxedValuePod((i16)DatumGetInt16(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Int32:
        return NUdf::TUnboxedValuePod((i32)DatumGetInt32(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Int64:
        return NUdf::TUnboxedValuePod((i64)DatumGetInt64(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Float:
        return NUdf::TUnboxedValuePod((float)DatumGetFloat4(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Double:
        return NUdf::TUnboxedValuePod((double)DatumGetFloat8(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8:
        if (IsCString) {
            auto x = (const char*)PointerDatumFromPod(value);
            return MakeString(TStringBuf(x));
        } else {
            auto x = (const text*)PointerDatumFromPod(value);
            return MakeString(GetVarBuf(x));
        }
    default:
        ythrow yexception() << "Unexpected data slot in ConvertFromPgValue: " << Slot;
    }
}

NUdf::TUnboxedValuePod ConvertFromPgValue(NUdf::TUnboxedValuePod source, ui32 sourceTypeId, NKikimr::NMiniKQL::TType* targetType) {
    TMaybe<NUdf::EDataSlot> targetDataTypeSlot;
#ifndef NDEBUG
    bool isOptional = false;
    auto targetDataType = UnpackOptionalData(targetType, isOptional);
    YQL_ENSURE(targetDataType);

    targetDataTypeSlot = targetDataType->GetDataSlot();
    if (!source && !isOptional) {
        throw yexception() << "Null value is not allowed for non-optional data type " << *targetType;
    }
#else
    Y_UNUSED(targetType);
#endif

    if (!source) {
        return source;
    }

    switch (sourceTypeId) {
    case BOOLOID:
        return ConvertFromPgValue<NUdf::EDataSlot::Bool, false>(source, targetDataTypeSlot);
    case INT2OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Int16, false>(source, targetDataTypeSlot);
    case INT4OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Int32, false>(source, targetDataTypeSlot);
    case INT8OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Int64, false>(source, targetDataTypeSlot);
    case FLOAT4OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Float, false>(source, targetDataTypeSlot);
    case FLOAT8OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Double, false>(source, targetDataTypeSlot);
    case TEXTOID:
    case VARCHAROID:
        return ConvertFromPgValue<NUdf::EDataSlot::Utf8, false>(source, targetDataTypeSlot);
    case BYTEAOID:
        return ConvertFromPgValue<NUdf::EDataSlot::String, false>(source, targetDataTypeSlot);
    case CSTRINGOID:
        return ConvertFromPgValue<NUdf::EDataSlot::Utf8, true>(source, targetDataTypeSlot);
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(sourceTypeId).Name;
    }
}

NUdf::TUnboxedValuePod ConvertToPgValue(NUdf::TUnboxedValuePod source, NKikimr::NMiniKQL::TType* sourceType, ui32 targetTypeId) {
    TMaybe<NUdf::EDataSlot> sourceDataTypeSlot;
#ifndef NDEBUG
    bool isOptional = false;
    auto sourceDataType = UnpackOptionalData(sourceType, isOptional);
    YQL_ENSURE(sourceDataType);
    sourceDataTypeSlot = sourceDataType->GetDataSlot();

    if (!source && !isOptional) {
        throw yexception() << "Null value is not allowed for non-optional data type " << *sourceType;
    }
#else
    Y_UNUSED(sourceType);
#endif

    if (!source) {
        return source;
    }

    switch (targetTypeId) {
    case BOOLOID:
        return ConvertToPgValue<NUdf::EDataSlot::Bool>(source, sourceDataTypeSlot);
    case INT2OID:
        return ConvertToPgValue<NUdf::EDataSlot::Int16>(source, sourceDataTypeSlot);
    case INT4OID:
        return ConvertToPgValue<NUdf::EDataSlot::Int32>(source, sourceDataTypeSlot);
    case INT8OID:
        return ConvertToPgValue<NUdf::EDataSlot::Int64>(source, sourceDataTypeSlot);
    case FLOAT4OID:
        return ConvertToPgValue<NUdf::EDataSlot::Float>(source, sourceDataTypeSlot);
    case FLOAT8OID:
        return ConvertToPgValue<NUdf::EDataSlot::Double>(source, sourceDataTypeSlot);
    case TEXTOID:
        return ConvertToPgValue<NUdf::EDataSlot::Utf8>(source, sourceDataTypeSlot);
    case BYTEAOID:
        return ConvertToPgValue<NUdf::EDataSlot::String>(source, sourceDataTypeSlot);
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(targetTypeId).Name;
    }
}

template <NUdf::EDataSlot Slot, bool IsCString>
class TFromPg : public TMutableComputationNode<TFromPg<Slot, IsCString>> {
    typedef TMutableComputationNode<TFromPg<Slot, IsCString>> TBaseComputation;
public:
    TFromPg(TComputationMutables& mutables, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg(arg)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value) {
            return value.Release();
        }
        return ConvertFromPgValue<Slot, IsCString>(value);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    IComputationNode* const Arg;
};

template <NUdf::EDataSlot Slot>
class TToPg : public TMutableComputationNode<TToPg<Slot>> {
    typedef TMutableComputationNode<TToPg<Slot>> TBaseComputation;
public:
    TToPg(TComputationMutables& mutables, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg(arg)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value) {
            return value.Release();
        }

        return ConvertToPgValue<Slot>(value);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    IComputationNode* const Arg;
};

class TPgArray : public TMutableComputationNode<TPgArray> {
    typedef TMutableComputationNode<TPgArray> TBaseComputation;
public:
    TPgArray(TComputationMutables& mutables, TComputationNodePtrVector&& argNodes, const TVector<TType*>&& argTypes, ui32 arrayType)
        : TBaseComputation(mutables)
        , ArgNodes(std::move(argNodes))
        , ArgTypes(std::move(argTypes))
        , ArrayTypeDesc(NPg::LookupType(arrayType))
        , ElemTypeDesc(NPg::LookupType(ArrayTypeDesc.ElementTypeId))
    {
        ArgDescs.resize(ArgNodes.size());
        for (ui32 i = 0; i < ArgNodes.size(); ++i) {
            if (!ArgTypes[i]->IsNull()) {
                auto type = static_cast<TPgType*>(ArgTypes[i])->GetTypeId();
                ArgDescs[i] = NPg::LookupType(type);
                if (ArgDescs[i].TypeId == ArgDescs[i].ArrayTypeId) {
                    MultiDims = true;
                }
            }
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        TUnboxedValueVector args;
        ui32 nelems = ArgNodes.size();
        args.reserve(nelems);
        for (ui32 i = 0; i < nelems; ++i) {
            auto value = ArgNodes[i]->GetValue(compCtx);
            args.push_back(value);
        }

        Datum* dvalues = (Datum*)TWithDefaultMiniKQLAlloc::AllocWithSize(nelems * sizeof(Datum));
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(dvalues, nelems * sizeof(Datum));
        };

        bool *dnulls = (bool*)TWithDefaultMiniKQLAlloc::AllocWithSize(nelems);
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(dnulls, nelems);
        };

        TPAllocScope call;
        for (ui32 i = 0; i < nelems; ++i) {
            const auto& value = args[i];
            if (value) {
                dnulls[i] = false;

                dvalues[i] = ArgDescs[i].PassByValue ?
                    ScalarDatumFromPod(value) :
                    PointerDatumFromPod(value);
            } else {
                dnulls[i] = true;
            }
        }

        {
            int ndims = 0;
            int dims[MAXDIM];
            int lbs[MAXDIM];
            if (!MultiDims) {
                // 1D array
                ndims = 1;
                dims[0] = nelems;
                lbs[0] = 1;

                auto result = construct_md_array(dvalues, dnulls, ndims, dims, lbs,
                    ElemTypeDesc.TypeId,
                    ElemTypeDesc.TypeLen,
                    ElemTypeDesc.PassByValue,
                    ElemTypeDesc.TypeAlign);
                return PointerDatumToPod(PointerGetDatum(result));
            }
            else {
                /* Must be nested array expressions */
                auto element_type = ElemTypeDesc.TypeId;
                int nbytes = 0;
                int nitems = 0;
                int outer_nelems = 0;
                int elem_ndims = 0;
                int *elem_dims = NULL;
                int *elem_lbs = NULL;

                bool firstone = true;
                bool havenulls = false;
                bool haveempty = false;
                char **subdata;
                bits8 **subbitmaps;
                int *subbytes;
                int *subnitems;
                int32 dataoffset;
                char *dat;
                int iitem;

                subdata = (char **)palloc(nelems * sizeof(char *));
                subbitmaps = (bits8 **)palloc(nelems * sizeof(bits8 *));
                subbytes = (int *)palloc(nelems * sizeof(int));
                subnitems = (int *)palloc(nelems * sizeof(int));

                /* loop through and get data area from each element */
                for (int elemoff = 0; elemoff < nelems; elemoff++)
                {
                    Datum arraydatum;
                    bool eisnull;
                    ArrayType *array;
                    int this_ndims;

                    arraydatum = dvalues[elemoff];
                    eisnull = dnulls[elemoff];

                    /* temporarily ignore null subarrays */
                    if (eisnull)
                    {
                        haveempty = true;
                        continue;
                    }

                    array = DatumGetArrayTypeP(arraydatum);

                    /* run-time double-check on element type */
                    if (element_type != ARR_ELEMTYPE(array))
                        ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("cannot merge incompatible arrays"),
                            errdetail("Array with element type %s cannot be "
                                "included in ARRAY construct with element type %s.",
                                format_type_be(ARR_ELEMTYPE(array)),
                                format_type_be(element_type))));

                    this_ndims = ARR_NDIM(array);
                    /* temporarily ignore zero-dimensional subarrays */
                    if (this_ndims <= 0)
                    {
                        haveempty = true;
                        continue;
                    }

                    if (firstone)
                    {
                        /* Get sub-array details from first member */
                        elem_ndims = this_ndims;
                        ndims = elem_ndims + 1;
                        if (ndims <= 0 || ndims > MAXDIM)
                            ereport(ERROR,
                            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                                errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                                    ndims, MAXDIM)));

                        elem_dims = (int *)palloc(elem_ndims * sizeof(int));
                        memcpy(elem_dims, ARR_DIMS(array), elem_ndims * sizeof(int));
                        elem_lbs = (int *)palloc(elem_ndims * sizeof(int));
                        memcpy(elem_lbs, ARR_LBOUND(array), elem_ndims * sizeof(int));

                        firstone = false;
                    }
                    else
                    {
                        /* Check other sub-arrays are compatible */
                        if (elem_ndims != this_ndims ||
                            memcmp(elem_dims, ARR_DIMS(array),
                                elem_ndims * sizeof(int)) != 0 ||
                            memcmp(elem_lbs, ARR_LBOUND(array),
                                elem_ndims * sizeof(int)) != 0)
                            ereport(ERROR,
                            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                                errmsg("multidimensional arrays must have array "
                                    "expressions with matching dimensions")));
                    }

                    subdata[outer_nelems] = ARR_DATA_PTR(array);
                    subbitmaps[outer_nelems] = ARR_NULLBITMAP(array);
                    subbytes[outer_nelems] = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
                    nbytes += subbytes[outer_nelems];
                    subnitems[outer_nelems] = ArrayGetNItems(this_ndims,
                        ARR_DIMS(array));
                    nitems += subnitems[outer_nelems];
                    havenulls |= ARR_HASNULL(array);
                    outer_nelems++;
                }

                /*
                 * If all items were null or empty arrays, return an empty array;
                 * otherwise, if some were and some weren't, raise error.  (Note: we
                 * must special-case this somehow to avoid trying to generate a 1-D
                 * array formed from empty arrays.  It's not ideal...)
                 */
                if (haveempty)
                {
                    if (ndims == 0) /* didn't find any nonempty array */
                    {
                        return PointerDatumToPod(PointerGetDatum(construct_empty_array(element_type)));
                    }
                    ereport(ERROR,
                        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmsg("multidimensional arrays must have array "
                                "expressions with matching dimensions")));
                }

                /* setup for multi-D array */
                dims[0] = outer_nelems;
                lbs[0] = 1;
                for (int i = 1; i < ndims; i++)
                {
                    dims[i] = elem_dims[i - 1];
                    lbs[i] = elem_lbs[i - 1];
                }

                /* check for subscript overflow */
                (void)ArrayGetNItems(ndims, dims);
                ArrayCheckBounds(ndims, dims, lbs);

                if (havenulls)
                {
                    dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
                    nbytes += dataoffset;
                }
                else
                {
                    dataoffset = 0; /* marker for no null bitmap */
                    nbytes += ARR_OVERHEAD_NONULLS(ndims);
                }

                ArrayType* result = (ArrayType *)palloc(nbytes);
                SET_VARSIZE(result, nbytes);
                result->ndim = ndims;
                result->dataoffset = dataoffset;
                result->elemtype = element_type;
                memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
                memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

                dat = ARR_DATA_PTR(result);
                iitem = 0;
                for (int i = 0; i < outer_nelems; i++)
                {
                    memcpy(dat, subdata[i], subbytes[i]);
                    dat += subbytes[i];
                    if (havenulls)
                        array_bitmap_copy(ARR_NULLBITMAP(result), iitem,
                            subbitmaps[i], 0,
                            subnitems[i]);
                    iitem += subnitems[i];
                }

                return PointerDatumToPod(PointerGetDatum(result));
            }
        }
    }

private:
    void RegisterDependencies() const final {
        for (auto arg : ArgNodes) {
            DependsOn(arg);
        }
    }

    TComputationNodePtrVector ArgNodes;
    TVector<TType*> ArgTypes;
    const NPg::TTypeDesc& ArrayTypeDesc;
    const NPg::TTypeDesc& ElemTypeDesc;
    TVector<NPg::TTypeDesc> ArgDescs;
    bool MultiDims = false;
};

template <bool PassByValue>
class TPgClone : public TMutableComputationNode<TPgClone<PassByValue>> {
    typedef TMutableComputationNode<TPgClone<PassByValue>> TBaseComputation;
public:
    TPgClone(TComputationMutables& mutables, IComputationNode* input, TComputationNodePtrVector&& dependentNodes, i32 typeLen)
        : TBaseComputation(mutables)
        , Input(input)
        , DependentNodes(std::move(dependentNodes))
        , TypeLen(typeLen)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Input->GetValue(compCtx);
        if constexpr (PassByValue) {
            return value.Release();
        }

        auto datum = PointerDatumFromPod(value);
        if (TypeLen == -1) {
            return PointerDatumToPod((Datum)MakeVar(GetVarBuf((const text*)datum)));
        } else if (TypeLen == -2) {
            return PointerDatumToPod((Datum)MakeCString(TStringBuf((const char*)datum)));
        } else {
            return PointerDatumToPod((Datum)MakeFixedString(TStringBuf((const char*)datum), TypeLen));
        }
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Input);
        for (auto arg : DependentNodes) {
            this->DependsOn(arg);
        }
    }

    IComputationNode* const Input;
    TComputationNodePtrVector DependentNodes;
    const i32 TypeLen;
};

struct TFromPgExec {
    TFromPgExec(ui32 sourceId)
        : SourceId(sourceId)
        , IsCString(NPg::LookupType(sourceId).TypeLen == -2)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        Y_ENSURE(inputDatum.is_array());
        const auto& array= *inputDatum.array();
        size_t length = array.length;
        switch (SourceId) {
        case BOOLOID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<ui8>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetBool(inputPtr[i]) ? 1 : 0;
            }
            break;
        }
        case INT2OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<i16>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetInt16(inputPtr[i]);
            }
            break;
        }
        case INT4OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<i32>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetInt32(inputPtr[i]);
            }
            break;
        }
        case INT8OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<i64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetInt64(inputPtr[i]);
            }
            break;
        }
        case FLOAT4OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<float>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetFloat4(inputPtr[i]);
            }
            break;
        }
        case FLOAT8OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<double>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetFloat8(inputPtr[i]);
            }
            break;
        }
        case TEXTOID:
        case VARCHAROID:
        case BYTEAOID:
        case CSTRINGOID: {
            NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
            NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), SourceId == BYTEAOID ? arrow::binary() : arrow::utf8(), *ctx->memory_pool(), length);
            for (size_t i = 0; i < length; ++i) {
                auto item = reader.GetItem(array, i);
                if (!item) {
                    builder.Add(NUdf::TBlockItem());
                    continue;
                }

                ui32 len;
                const char* ptr = item.AsStringRef().Data() + sizeof(void*);
                if (IsCString) {
                    len = strlen(ptr);
                } else {
                    len = GetCleanVarSize((const text*)ptr);
                    Y_ENSURE(len + VARHDRSZ + sizeof(void*) == item.AsStringRef().Size());
                    ptr += VARHDRSZ;
                }

                builder.Add(NUdf::TBlockItem(NUdf::TStringRef(ptr, len)));
            }

            *res = builder.Build(true);
            break;
        }
        default:
            ythrow yexception() << "Unsupported type: " << NPg::LookupType(SourceId).Name;
        }
        return arrow::Status::OK();
    }

    const ui32 SourceId;
    const bool IsCString;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeFromPgKernel(TType* inputType, TType* resultType, ui32 sourceId) {
    const TVector<TType*> argTypes = { inputType };

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TFromPgExec>(sourceId);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    switch (sourceId) {
    case BOOLOID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
        break;
    case TEXTOID:
    case VARCHAROID:
    case BYTEAOID:
    case CSTRINGOID:
        kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
        break;
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(sourceId).Name;
    }

    return kernel;
}

struct TToPgExec {
    TToPgExec(ui32 targetId)
        : TargetId(targetId)
        , IsCString(NPg::LookupType(targetId).TypeLen == -2)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        Y_ENSURE(inputDatum.is_array());
        const auto& array= *inputDatum.array();
        size_t length = array.length;
        switch (TargetId) {
        case BOOLOID: {
            auto inputPtr = array.GetValues<ui8>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = BoolGetDatum(inputPtr[i]);
            }
            break;
        }
        case INT2OID: {
            auto inputPtr = array.GetValues<i16>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Int16GetDatum(inputPtr[i]);
            }
            break;
        }
        case INT4OID: {
            auto inputPtr = array.GetValues<i32>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Int32GetDatum(inputPtr[i]);
            }
            break;
        }
        case INT8OID: {
            auto inputPtr = array.GetValues<i64>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Int64GetDatum(inputPtr[i]);
            }
            break;
        }
        case FLOAT4OID: {
            auto inputPtr = array.GetValues<float>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Float4GetDatum(inputPtr[i]);
            }
            break;
        }
        case FLOAT8OID: {
            auto inputPtr = array.GetValues<double>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Float8GetDatum(inputPtr[i]);
            }
            break;
        }
        case TEXTOID:
        case VARCHAROID:
        case BYTEAOID:
        case CSTRINGOID: {
            NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
            NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
            std::vector<char> tmp;
            for (size_t i = 0; i < length; ++i) {
                auto item = reader.GetItem(array, i);
                if (!item) {
                    builder.Add(NUdf::TBlockItem());
                    continue;
                }

                ui32 len;
                if (IsCString) {
                    len = sizeof(void*) + 1 + item.AsStringRef().Size();
                    if (Y_UNLIKELY(len < item.AsStringRef().Size())) {
                        ythrow yexception() << "Too long string";
                    }

                    if (tmp.capacity() < len) {
                        tmp.reserve(Max<ui64>(len, tmp.capacity() * 2));
                    }

                    tmp.resize(len);
                    NUdf::ZeroMemoryContext(tmp.data() + sizeof(void*));
                    memcpy(tmp.data() + sizeof(void*), item.AsStringRef().Data(), len - 1 - sizeof(void*));
                    tmp[len - 1] = 0;
                } else {
                    len = sizeof(void*) + VARHDRSZ + item.AsStringRef().Size();
                    if (Y_UNLIKELY(len < item.AsStringRef().Size())) {
                        ythrow yexception() << "Too long string";
                    }

                    if (tmp.capacity() < len) {
                        tmp.reserve(Max<ui64>(len, tmp.capacity() * 2));
                    }

                    tmp.resize(len);
                    NUdf::ZeroMemoryContext(tmp.data() + sizeof(void*));
                    memcpy(tmp.data() + sizeof(void*) + VARHDRSZ, item.AsStringRef().Data(), len - VARHDRSZ - sizeof(void*));
                    UpdateCleanVarSize((text*)(tmp.data() + sizeof(void*)), item.AsStringRef().Size());
                }

                builder.Add(NUdf::TBlockItem(NUdf::TStringRef(tmp.data(), len)));
            }

            *res = builder.Build(true);
            break;
        }
        default:
            ythrow yexception() << "Unsupported type: " << NPg::LookupType(TargetId).Name;
        }
        return arrow::Status::OK();
    }

    const ui32 TargetId;
    const bool IsCString;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeToPgKernel(TType* inputType, TType* resultType, ui32 targetId) {
    const TVector<TType*> argTypes = { inputType };

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TToPgExec>(targetId);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    switch (targetId) {
    case BOOLOID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
        break;
    case TEXTOID:
    case VARCHAROID:
    case BYTEAOID:
    case CSTRINGOID:
        kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
        break;
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(targetId).Name;
    }

    return kernel;
}

std::shared_ptr<arrow::compute::ScalarKernel> MakePgKernel(TVector<TType*> argTypes, TType* resultType, TExecFunc execFunc, ui32 procId) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [execFunc](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return execFunc(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->init = [procId](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
        auto state = std::make_unique<TPgKernelState>();
        Zero(state->flinfo);
        fmgr_info(procId, &state->flinfo);
        YQL_ENSURE(state->flinfo.fn_addr);
        state->resultinfo = nullptr;
        state->context = nullptr;
        state->fncollation = DEFAULT_COLLATION_OID;
        const auto& procDesc = NPg::LookupProc(procId);
        const auto& retTypeDesc = NPg::LookupType(procDesc.ResultType);
        state->Name = procDesc.Name;
        state->IsFixedResult = retTypeDesc.PassByValue;
        state->TypeLen = retTypeDesc.TypeLen;
        for (const auto& argTypeId : procDesc.ArgTypes) {
            const auto& argTypeDesc = NPg::LookupType(argTypeId);
            state->IsFixedArg.push_back(argTypeDesc.PassByValue);
        }

        return arrow::Result(std::move(state));
    };

    return kernel;
}

TComputationNodeFactory GetPgFactory() {
    return [] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            TStringBuf name = callable.GetType()->GetName();
            if (name == "PgConst") {
                const auto typeIdData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto valueData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                ui32 typeId = typeIdData->AsValue().Get<ui32>();
                auto value = valueData->AsValue().AsStringRef();
                IComputationNode* typeMod = nullptr;
                if (callable.GetInputsCount() >= 3) {
                    typeMod = LocateNode(ctx.NodeLocator, callable, 2);
                }

                return new TPgConst(ctx.Mutables, typeId, value, typeMod);
            }

            if (name == "PgInternal0") {
                return new TPgInternal0(ctx.Mutables);
            }

            if (name == "PgTableContent") {
                const auto clusterData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto tableData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                const auto cluster = clusterData->AsValue().AsStringRef();
                const auto table = tableData->AsValue().AsStringRef();
                const auto returnType = callable.GetType()->GetReturnType();
                return new TPgTableContent(ctx.Mutables, cluster, table, returnType);
            }

            if (name == "PgResolvedCall") {
                const auto useContextData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto rangeFunctionData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                const auto nameData = AS_VALUE(TDataLiteral, callable.GetInput(2));
                const auto idData = AS_VALUE(TDataLiteral, callable.GetInput(3));
                auto useContext = useContextData->AsValue().Get<bool>();
                auto rangeFunction = rangeFunctionData->AsValue().Get<bool>();
                auto name = nameData->AsValue().AsStringRef();
                auto id = idData->AsValue().Get<ui32>();
                TComputationNodePtrVector argNodes;
                TVector<TType*> argTypes;
                for (ui32 i = 4; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                    argTypes.emplace_back(callable.GetInput(i).GetStaticType());
                }

                const auto returnType = callable.GetType()->GetReturnType();
                const bool isList = returnType->IsList();
                const auto itemType = isList ? AS_TYPE(TListType, returnType)->GetItemType() : returnType;
                const TStructType* structType = nullptr;
                if (rangeFunction) {
                    if (itemType->IsStruct()) {
                        structType = AS_TYPE(TStructType, itemType);
                    }
                }

                if (isList) {
                    YQL_ENSURE(!useContext);
                    return new TPgResolvedMultiCall(ctx.Mutables, name, id, std::move(argNodes), std::move(argTypes), structType);
                } else {
                    YQL_ENSURE(!structType);
                    if (useContext) {
                        return new TPgResolvedCall<true>(ctx.Mutables, name, id, std::move(argNodes), std::move(argTypes));
                    } else {
                        return new TPgResolvedCall<false>(ctx.Mutables, name, id, std::move(argNodes), std::move(argTypes));
                    }
                }
            }

            if (name == "BlockPgResolvedCall") {
                const auto nameData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto idData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                auto name = nameData->AsValue().AsStringRef();
                auto id = idData->AsValue().Get<ui32>();
                TComputationNodePtrVector argNodes;
                TVector<TType*> argTypes;
                for (ui32 i = 2; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                    argTypes.emplace_back(callable.GetInput(i).GetStaticType());
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto execFunc = FindExec(id);
                YQL_ENSURE(execFunc);
                auto kernel = MakePgKernel(argTypes, returnType, execFunc, id);
                return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argNodes), argTypes, *kernel, kernel);
            }

            if (name == "PgCast") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                ui32 sourceId = 0;
                if (!inputType->IsNull()) {
                    sourceId = AS_TYPE(TPgType, inputType)->GetTypeId();
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, returnType)->GetTypeId();
                IComputationNode* typeMod = nullptr;
                if (callable.GetInputsCount() >= 2) {
                    typeMod = LocateNode(ctx.NodeLocator, callable, 1);
                }

                return new TPgCast(ctx.Mutables, sourceId, targetId, arg, typeMod);
            }

            if (name == "FromPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                ui32 sourceId = AS_TYPE(TPgType, inputType)->GetTypeId();
                switch (sourceId) {
                case BOOLOID:
                    return new TFromPg<NUdf::EDataSlot::Bool, false>(ctx.Mutables, arg);
                case INT2OID:
                    return new TFromPg<NUdf::EDataSlot::Int16, false>(ctx.Mutables, arg);
                case INT4OID:
                    return new TFromPg<NUdf::EDataSlot::Int32, false>(ctx.Mutables, arg);
                case INT8OID:
                    return new TFromPg<NUdf::EDataSlot::Int64, false>(ctx.Mutables, arg);
                case FLOAT4OID:
                    return new TFromPg<NUdf::EDataSlot::Float, false>(ctx.Mutables, arg);
                case FLOAT8OID:
                    return new TFromPg<NUdf::EDataSlot::Double, false>(ctx.Mutables, arg);
                case TEXTOID:
                case VARCHAROID:
                    return new TFromPg<NUdf::EDataSlot::Utf8, false>(ctx.Mutables, arg);
                case BYTEAOID:
                    return new TFromPg<NUdf::EDataSlot::String, false>(ctx.Mutables, arg);
                case CSTRINGOID:
                    return new TFromPg<NUdf::EDataSlot::Utf8, true>(ctx.Mutables, arg);
                default:
                    ythrow yexception() << "Unsupported type: " << NPg::LookupType(sourceId).Name;
                }
            }

            if (name == "BlockFromPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                auto returnType = callable.GetType()->GetReturnType();
                ui32 sourceId = AS_TYPE(TPgType, AS_TYPE(TBlockType, inputType)->GetItemType())->GetTypeId();
                auto kernel = MakeFromPgKernel(inputType, returnType, sourceId);
                return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), { arg }, { inputType }, *kernel, kernel);
            }

            if (name == "ToPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, returnType)->GetTypeId();
                switch (targetId) {
                case BOOLOID:
                    return new TToPg<NUdf::EDataSlot::Bool>(ctx.Mutables, arg);
                case INT2OID:
                    return new TToPg<NUdf::EDataSlot::Int16>(ctx.Mutables, arg);
                case INT4OID:
                    return new TToPg<NUdf::EDataSlot::Int32>(ctx.Mutables, arg);
                case INT8OID:
                    return new TToPg<NUdf::EDataSlot::Int64>(ctx.Mutables, arg);
                case FLOAT4OID:
                    return new TToPg<NUdf::EDataSlot::Float>(ctx.Mutables, arg);
                case FLOAT8OID:
                    return new TToPg<NUdf::EDataSlot::Double>(ctx.Mutables, arg);
                case TEXTOID:
                    return new TToPg<NUdf::EDataSlot::Utf8>(ctx.Mutables, arg);
                case BYTEAOID:
                    return new TToPg<NUdf::EDataSlot::String>(ctx.Mutables, arg);
                default:
                    ythrow yexception() << "Unsupported type: " << NPg::LookupType(targetId).Name;
                }
            }

            if (name == "BlockToPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, AS_TYPE(TBlockType, returnType)->GetItemType())->GetTypeId();
                auto kernel = MakeToPgKernel(inputType, returnType, targetId);
                return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), { arg }, { inputType }, *kernel, kernel);
            }

            if (name == "PgArray") {
                TComputationNodePtrVector argNodes;
                TVector<TType*> argTypes;
                for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                    argTypes.emplace_back(callable.GetInput(i).GetStaticType());
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto arrayTypeId = AS_TYPE(TPgType, returnType)->GetTypeId();
                return new TPgArray(ctx.Mutables, std::move(argNodes), std::move(argTypes), arrayTypeId);
            }

            if (name == "PgClone") {
                auto input = LocateNode(ctx.NodeLocator, callable, 0);
                TComputationNodePtrVector dependentNodes;
                for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
                    dependentNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto typeId = AS_TYPE(TPgType, returnType)->GetTypeId();
                const auto& desc = NPg::LookupType(typeId);
                if (desc.PassByValue) {
                    return new TPgClone<true>(ctx.Mutables, input, std::move(dependentNodes), desc.TypeLen);
                } else if (desc.TypeLen == -1) {
                    return new TPgClone<false>(ctx.Mutables, input, std::move(dependentNodes), desc.TypeLen);
                } else {
                    return new TPgClone<false>(ctx.Mutables, input, std::move(dependentNodes), desc.TypeLen);
                }
            }

            return nullptr;
        };
}

namespace NCommon {

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    YQL_ENSURE(value); // null could not be represented as text

    TPAllocScope call;
    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto outFuncId = typeInfo.OutFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        outFuncId = NPg::LookupProc("array_out", { 0 }).ProcId;
    }

    char* str = nullptr;
    Y_DEFER {
        if (str) {
            pfree(str);
        }
    };

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(outFuncId);
        fmgr_info(outFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs == 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { typeInfo.PassByValue ?
            ScalarDatumFromPod(value) :
            PointerDatumFromPod(value), false };
        str = (char*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);

        return TString(str);
    }
}

template <typename F>
void PgValueToNativeBinaryImpl(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId, bool needCanonizeFp, F f) {
    YQL_ENSURE(value); // null could not be represented as binary

    const bool oldNeedCanonizeFp = NeedCanonizeFp;
    NeedCanonizeFp = needCanonizeFp;
    Y_DEFER {
        NeedCanonizeFp = oldNeedCanonizeFp;
    };

    TPAllocScope call;
    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto sendFuncId = typeInfo.SendFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        sendFuncId = NPg::LookupProc("array_send", { 0 }).ProcId;
    }

    text* x = nullptr;
    Y_DEFER {
        if (x) {
            pfree(x);
        }
    };

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(sendFuncId);
        fmgr_info(sendFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs == 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { typeInfo.PassByValue ?
            ScalarDatumFromPod(value) :
            PointerDatumFromPod(value), false };

        x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);

        auto s = GetVarBuf(x);
        ui32 len = s.Size();
        f(TStringBuf(s.Data(), s.Size()));
    }
}

TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    TString result;
    PgValueToNativeBinaryImpl(value, pgTypeId, false, [&result](TStringBuf b) {
        result = b;
    });
    return result;
}

TString PgValueToString(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    YQL_ENSURE(value); // null could not be represented as text

    switch (pgTypeId) {
    case BOOLOID:
        return DatumGetBool(ScalarDatumFromPod(value)) ? "true" : "false";
    case INT2OID:
        return ToString(DatumGetInt16(ScalarDatumFromPod(value)));
    case INT4OID:
        return ToString(DatumGetInt32(ScalarDatumFromPod(value)));
    case INT8OID:
        return ToString(DatumGetInt64(ScalarDatumFromPod(value)));
    case FLOAT4OID:
        return ::FloatToString(DatumGetFloat4(ScalarDatumFromPod(value)));
    case FLOAT8OID:
        return ::FloatToString(DatumGetFloat8(ScalarDatumFromPod(value)));
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        return TString(GetVarBuf(x));
    }
    case CSTRINGOID: {
        return TString((const char*)PointerDatumFromPod(value));
    }
    default:
        return PgValueToNativeText(value, pgTypeId);
    }
}

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, TPgType* type, const NUdf::TUnboxedValuePod& value) {
    using namespace NYson::NDetail;
    if (!value) {
        buf.Write(EntitySymbol);
        return;
    }

    switch (type->GetTypeId()) {
    case BOOLOID:
        buf.Write(DatumGetBool(ScalarDatumFromPod(value)) ? TrueMarker : FalseMarker);
        break;
    case INT2OID:
        buf.Write(Int64Marker);
        buf.WriteVarI64(DatumGetInt16(ScalarDatumFromPod(value)));
        break;
    case INT4OID:
        buf.Write(Int64Marker);
        buf.WriteVarI64(DatumGetInt32(ScalarDatumFromPod(value)));
        break;
    case INT8OID:
        buf.Write(Int64Marker);
        buf.WriteVarI64(DatumGetInt64(ScalarDatumFromPod(value)));
        break;
    case FLOAT4OID: {
        buf.Write(DoubleMarker);
        double val = DatumGetFloat4(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&val, sizeof(val));
        break;
    }
    case FLOAT8OID: {
        buf.Write(DoubleMarker);
        double val = DatumGetFloat8(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&val, sizeof(val));
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        buf.Write(StringMarker);
        buf.WriteVarI32(s.Size());
        buf.WriteMany(s.Data(), s.Size());
        break;
    }
    case CSTRINGOID: {
        auto s = (const char*)PointerDatumFromPod(value);
        auto len = strlen(s);
        buf.Write(StringMarker);
        buf.WriteVarI32(len);
        buf.WriteMany(s, len);
        break;
    }
    default:
        buf.Write(StringMarker);
        PgValueToNativeBinaryImpl(value, type->GetTypeId(), true, [&buf](TStringBuf b) {
            buf.WriteVarI32(b.Size());
            buf.WriteMany(b.Data(), b.Size());
        });
        break;
    }
}

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, TPgType* type,
    const TVector<ui32>* structPositions) {
    if (!value) {
        writer.OnNull();
        return;
    }

    writer.OnStringScalar(PgValueToString(value, type->GetTypeId()));
}

NUdf::TUnboxedValue ReadYsonValueInTableFormatPg(TPgType* type, char cmd, TInputBuf& buf) {
    using namespace NYson::NDetail;
    if (cmd == EntitySymbol) {
        return NUdf::TUnboxedValuePod();
    }

    switch (type->GetTypeId()) {
    case BOOLOID: {
        YQL_ENSURE(cmd == FalseMarker || cmd == TrueMarker, "Expected either true or false, but got: " << TString(cmd).Quote());
        return ScalarDatumToPod(BoolGetDatum(cmd == TrueMarker));
    }
    case INT2OID: {
        CHECK_EXPECTED(cmd, Int64Marker);
        auto x = i16(buf.ReadVarI64());
        return ScalarDatumToPod(Int16GetDatum(x));
    }
    case INT4OID: {
        CHECK_EXPECTED(cmd, Int64Marker);
        auto x = i32(buf.ReadVarI64());
        return ScalarDatumToPod(Int32GetDatum(x));
    }
    case INT8OID: {
        CHECK_EXPECTED(cmd, Int64Marker);
        auto x = buf.ReadVarI64();
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        CHECK_EXPECTED(cmd, DoubleMarker);
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float4GetDatum(x));
    }
    case FLOAT8OID: {
        CHECK_EXPECTED(cmd, DoubleMarker);
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        CHECK_EXPECTED(cmd, StringMarker);
        auto s = buf.ReadYtString();
        auto ret = MakeVar(s);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        CHECK_EXPECTED(cmd, StringMarker);
        auto s = buf.ReadYtString();
        auto ret = MakeCString(s);
        return PointerDatumToPod((Datum)ret);
    }
    default:
        TPAllocScope call;
        auto s = buf.ReadYtString();
        return PgValueFromNativeBinary(s, type->GetTypeId());
    }
}

NUdf::TUnboxedValue PgValueFromNativeBinary(const TStringBuf binary, ui32 pgTypeId) {
    TPAllocScope call;
    StringInfoData stringInfo;
    stringInfo.data = (char*)binary.Data();
    stringInfo.len = binary.Size();
    stringInfo.maxlen = binary.Size();
    stringInfo.cursor = 0;

    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto typeIOParam = MakeTypeIOParam(typeInfo);
    auto receiveFuncId = typeInfo.ReceiveFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
    }

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(receiveFuncId);
        fmgr_info(receiveFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs >= 1 && finfo.fn_nargs <= 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)&stringInfo, false };
        callInfo->args[1] = { ObjectIdGetDatum(typeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto x = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        if (stringInfo.cursor != stringInfo.len) {
            TStringBuilder errMsg;
            errMsg << "Not all data has been consumed by 'recv' function: " << NPg::LookupProc(receiveFuncId).Name << ", data size: " << stringInfo.len << ", consumed size: " << stringInfo.cursor;
            UdfTerminate(errMsg.c_str());
        }
        return AnyDatumToPod(x, typeInfo.PassByValue);
    }
}

NUdf::TUnboxedValue PgValueFromNativeText(const TStringBuf text, ui32 pgTypeId) {
    TString str{ text };

    TPAllocScope call;
    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto typeIOParam = MakeTypeIOParam(typeInfo);
    auto inFuncId = typeInfo.InFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
    }

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(inFuncId);
        fmgr_info(inFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs >= 1 && finfo.fn_nargs <= 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)str.c_str(), false };
        callInfo->args[1] = { ObjectIdGetDatum(typeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto x = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return AnyDatumToPod(x, typeInfo.PassByValue);
    }
}

NUdf::TUnboxedValue PgValueFromString(const TStringBuf s, ui32 pgTypeId) {
    switch (pgTypeId) {
    case BOOLOID: {
        return ScalarDatumToPod(BoolGetDatum(FromString<bool>(s)));
    }
    case INT2OID: {
        return ScalarDatumToPod(Int16GetDatum(FromString<i16>(s)));
    }
    case INT4OID: {
        return ScalarDatumToPod(Int32GetDatum(FromString<i32>(s)));
    }
    case INT8OID: {
        return ScalarDatumToPod(Int64GetDatum(FromString<i64>(s)));
    }
    case FLOAT4OID: {
        return ScalarDatumToPod(Float4GetDatum(FromString<float>(s)));
    }
    case FLOAT8OID: {
        return ScalarDatumToPod(Float8GetDatum(FromString<double>(s)));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        auto ret = MakeVar(s);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        auto ret = MakeCString(s);
        return PointerDatumToPod((Datum)ret);
    }
    default:
        return PgValueFromNativeText(s, pgTypeId);
    }
}

NUdf::TUnboxedValue ReadYsonValuePg(TPgType* type, char cmd, TInputBuf& buf) {
    using namespace NYson::NDetail;
    if (cmd == EntitySymbol) {
        return NUdf::TUnboxedValuePod();
    }

    CHECK_EXPECTED(cmd, StringMarker);
    auto s = buf.ReadYtString();
    return PgValueFromString(s, type->GetTypeId());
}

void SkipSkiffPg(TPgType* type, NCommon::TInputBuf& buf) {
    auto marker = buf.Read();
    if (!marker) {
        return;
    }

    switch (type->GetTypeId()) {
    case BOOLOID: {
        buf.Read();
        return;
    }
    case INT2OID:
    case INT4OID:
    case INT8OID: {
        buf.SkipMany(sizeof(i64));
        return;
    }
    case FLOAT4OID:
    case FLOAT8OID: {
        buf.SkipMany(sizeof(double));
        return;
    }
    default: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        buf.SkipMany(size);
        return;
    }
    }
}

NUdf::TUnboxedValue ReadSkiffPg(TPgType* type, NCommon::TInputBuf& buf) {
    auto marker = buf.Read();
    if (!marker) {
        return NUdf::TUnboxedValue();
    }

    switch (type->GetTypeId()) {
    case BOOLOID: {
        auto x = buf.Read();
        return ScalarDatumToPod(BoolGetDatum(x != 0));
    }
    case INT2OID: {
        i64 x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Int16GetDatum((i16)x));
    }
    case INT4OID: {
        i64 x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Int32GetDatum((i32)x));
    }
    case INT8OID: {
        i64 x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float4GetDatum((float)x));
    }
    case FLOAT8OID: {
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        text* s = (text*)palloc(size + VARHDRSZ);
        auto mem = s;
        Y_DEFER {
            if (mem) {
                pfree(mem);
            }
        };

        UpdateCleanVarSize(s, size);
        buf.ReadMany(GetMutableVarData(s), size);
        mem = nullptr;

        return PointerDatumToPod((Datum)s);
    }

    case CSTRINGOID: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        char* s = (char*)palloc(size + 1);
        auto mem = s;
        Y_DEFER {
            if (mem) {
                pfree(mem);
            }
        };

        buf.ReadMany(s, size);
        mem = nullptr;
        s[size] = '\0';

        return PointerDatumToPod((Datum)s);
    }
    default:
        TPAllocScope call;
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        char* s = (char*)TWithDefaultMiniKQLAlloc::AllocWithSize(size);
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(s, size);
        };

        buf.ReadMany(s, size);
        return PgValueFromNativeBinary(TStringBuf(s, size), type->GetTypeId());
    }
}

void WriteSkiffPg(TPgType* type, const NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    if (!value) {
        buf.Write('\0');
        return;
    }

    buf.Write('\1');
    switch (type->GetTypeId()) {
    case BOOLOID: {
        char x = DatumGetBool(ScalarDatumFromPod(value));
        buf.Write(x);
        break;
    }
    case INT2OID: {
        i64 x = DatumGetInt16(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case INT4OID: {
        i64 x = DatumGetInt32(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case INT8OID: {
        i64 x = DatumGetInt64(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case FLOAT4OID: {
        double x = DatumGetFloat4(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case FLOAT8OID: {
        double x = DatumGetFloat8(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        ui32 len = s.Size();
        buf.WriteMany((const char*)&len, sizeof(len));
        buf.WriteMany(s.Data(), len);
        break;
    }
    case CSTRINGOID: {
        const auto x = (const char*)PointerDatumFromPod(value);
        ui32 len = strlen(x);
        buf.WriteMany((const char*)&len, sizeof(len));
        buf.WriteMany(x, len);
        break;
    }
    default:
        PgValueToNativeBinaryImpl(value, type->GetTypeId(), true, [&buf](TStringBuf b) {
            ui32 len = b.Size();
            buf.WriteMany((const char*)&len, sizeof(len));
            buf.WriteMany(b.Data(), len);
        });
    }
}

extern "C" void ReadSkiffPgValue(TPgType* type, NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf) {
    value = ReadSkiffPg(type, buf);
}

extern "C" void WriteSkiffPgValue(TPgType* type, const NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    WriteSkiffPg(type, value, buf);
}

} // namespace NCommon

namespace {

template<typename TScalarGetter, typename TPointerGetter>
arrow::Datum DoMakePgScalar(const NPg::TTypeDesc& desc, arrow::MemoryPool& pool, const TScalarGetter& getScalar, const TPointerGetter& getPtr) {
    if (desc.PassByValue) {
        return arrow::MakeScalar(getScalar());
    } else {
        const char* ptr = getPtr();
        ui32 size;
        if (desc.TypeLen == -1) {
            size = GetCleanVarSize((const text*)ptr) + VARHDRSZ;
        } else if (desc.TypeLen == -2) {
            size = strlen(ptr) + 1;
        } else {
            size = desc.TypeLen;
        }

        std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(size + sizeof(void*), &pool)));
        NUdf::ZeroMemoryContext(buffer->mutable_data() + sizeof(void*));
        std::memcpy(buffer->mutable_data() + sizeof(void*), ptr, size);
        return arrow::Datum(std::make_shared<arrow::BinaryScalar>(buffer));
    }
}

} // namespace

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool) {
    return DoMakePgScalar(
        NPg::LookupType(type->GetTypeId()), pool,
        [&value]() { return (uint64_t)ScalarDatumFromPod(value); },
        [&value]() { return (const char*)PointerDatumFromPod(value); }
    );
}

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool) {
    return DoMakePgScalar(
        NPg::LookupType(type->GetTypeId()), pool,
        [&value]() { return (uint64_t)ScalarDatumFromItem(value); },
        [&value]() { return (const char*)PointerDatumFromItem(value); }
    );
}

TMaybe<ui32> ConvertToPgType(NUdf::EDataSlot slot) {
    switch (slot) {
    case NUdf::EDataSlot::Bool:
        return BOOLOID;
    case NUdf::EDataSlot::Int16:
        return INT2OID;
    case NUdf::EDataSlot::Int32:
        return INT4OID;
    case NUdf::EDataSlot::Int64:
        return INT8OID;
    case NUdf::EDataSlot::Float:
        return FLOAT4OID;
    case NUdf::EDataSlot::Double:
        return FLOAT8OID;
    case NUdf::EDataSlot::String:
        return BYTEAOID;
    case NUdf::EDataSlot::Utf8:
        return TEXTOID;
    default:
        return Nothing();
    }
}

TMaybe<NUdf::EDataSlot> ConvertFromPgType(ui32 typeId) {
    switch (typeId) {
    case BOOLOID:
        return NUdf::EDataSlot::Bool;
    case INT2OID:
        return NUdf::EDataSlot::Int16;
    case INT4OID:
        return NUdf::EDataSlot::Int32;
    case INT8OID:
        return NUdf::EDataSlot::Int64;
    case FLOAT4OID:
        return NUdf::EDataSlot::Float;
    case FLOAT8OID:
        return NUdf::EDataSlot::Double;
    case BYTEAOID:
        return NUdf::EDataSlot::String;
    case TEXTOID:
        return NUdf::EDataSlot::Utf8;
    }

    return Nothing();
}

bool ParsePgIntervalModifier(const TString& str, i32& ret) {
    auto ustr = to_upper(str);
    if (ustr == "YEAR") {
        ret = INTERVAL_MASK(YEAR);
    } else if (ustr == "MONTH") {
        ret = INTERVAL_MASK(MONTH);
    } else if (ustr == "DAY") {
        ret = INTERVAL_MASK(DAY);
    } else if (ustr == "HOUR") {
        ret = INTERVAL_MASK(HOUR);
    } else if (ustr == "MINUTE") {
        ret = INTERVAL_MASK(MINUTE);
    } else if (ustr == "SECOND") {
        ret = INTERVAL_MASK(SECOND);
    } else if (ustr == "YEAR TO MONTH") {
        ret = INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH);
    } else if (ustr == "DAY TO HOUR") {
        ret = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR);
    } else if (ustr == "DAY TO MINUTE") {
        ret = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE);
    } else if (ustr == "DAY TO SECOND") {
        ret = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);
    } else if (ustr == "HOUR TO MINUTE") {
        ret = INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE);
    } else if (ustr == "HOUR TO SECOND") {
        ret = INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);
    } else if (ustr == "MINUTE TO SECOND") {
        ret = INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);
    } else {
        return false;
    }

    return true;
}

template<typename TBuf>
void DoPGPack(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuf& buf) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = DatumGetBool(ScalarDatumFromPod(value)) != 0;
        NDetails::PutRawData(x, buf);
        break;
    }
    case INT2OID: {
        const auto x = DatumGetInt16(ScalarDatumFromPod(value));
        NDetails::PackInt16(x, buf);
        break;
    }
    case INT4OID: {
        const auto x = DatumGetInt32(ScalarDatumFromPod(value));
        NDetails::PackInt32(x, buf);
        break;
    }
    case INT8OID: {
        const auto x = DatumGetInt64(ScalarDatumFromPod(value));
        NDetails::PackInt64(x, buf);
        break;
    }
    case FLOAT4OID: {
        auto x = DatumGetFloat4(ScalarDatumFromPod(value));
        if (stable) {
            NYql::CanonizeFpBits<float>(&x);
        }

        NDetails::PutRawData(x, buf);
        break;
    }
    case FLOAT8OID: {
        auto x = DatumGetFloat8(ScalarDatumFromPod(value));
        if (stable) {
            NYql::CanonizeFpBits<double>(&x);
        }

        NDetails::PutRawData(x, buf);
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        NDetails::PackUInt32(s.Size(), buf);
        buf.Append(s.Data(), s.Size());
        break;
    }
    case CSTRINGOID: {
        const auto x = (const char*)PointerDatumFromPod(value);
        const auto len = strlen(x);
        NDetails::PackUInt32(len, buf);
        buf.Append(x, len);
        break;
    }
    default:
        NYql::NCommon::PgValueToNativeBinaryImpl(value, type->GetTypeId(), stable, [&buf](TStringBuf b) {
            NDetails::PackUInt32(b.Size(), buf);
            buf.Append(b.Data(), b.Size());
        });
    }
}

} // NYql


namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

ui64 PgValueSize(const NUdf::TUnboxedValuePod& value, i32 typeLen) {
    if (typeLen == -1) {
        auto datum = PointerDatumFromPod(value);
        const auto x = (const text*)PointerDatumFromPod(value);
        return GetCleanVarSize(x);
    } else if (typeLen == -2) {
        auto datum = PointerDatumFromPod(value);
        const auto x = (const char*)PointerDatumFromPod(value);
        return strlen(x);
    } else {
        return typeLen;
    }
}

ui64 PgValueSize(ui32 pgTypeId, const NUdf::TUnboxedValuePod& value) {
    const auto& typeDesc = NYql::NPg::LookupType(pgTypeId);
    return PgValueSize(value, typeDesc.TypeLen);
}

ui64 PgValueSize(const TPgType* type, const NUdf::TUnboxedValuePod& value) {
    return PgValueSize(type->GetTypeId(), value);
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
    DoPGPack(stable, type, value, buf);
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TPagedBuffer& buf) {
    DoPGPack(stable, type, value, buf);
}

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, TStringBuf& buf) {
    NDetails::TChunkedInputBuffer chunked(buf);
    return PGUnpackImpl(type, chunked);
}

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, NDetails::TChunkedInputBuffer& buf) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = NDetails::GetRawData<bool>(buf);
        return ScalarDatumToPod(BoolGetDatum(x));
    }
    case INT2OID: {
        const auto x = NDetails::UnpackInt16(buf);
        return ScalarDatumToPod(Int16GetDatum(x));
    }
    case INT4OID: {
        const auto x = NDetails::UnpackInt32(buf);
        return ScalarDatumToPod(Int32GetDatum(x));
    }
    case INT8OID: {
        const auto x = NDetails::UnpackInt64(buf);
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        const auto x = NDetails::GetRawData<float>(buf);
        return ScalarDatumToPod(Float4GetDatum(x));
    }
    case FLOAT8OID: {
        const auto x = NDetails::GetRawData<double>(buf);
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        auto size = NDetails::UnpackUInt32(buf);
        auto deleter = [](text* ptr) { pfree(ptr); };
        std::unique_ptr<text, decltype(deleter)> ret(MakeVarNotFilled(size));
        buf.CopyTo(GetMutableVarData(ret.get()), size);
        return PointerDatumToPod((Datum)ret.release());
    }
    case CSTRINGOID: {
        auto size = NDetails::UnpackUInt32(buf);
        auto deleter = [](char* ptr) { pfree(ptr); };
        std::unique_ptr<char, decltype(deleter)> ret(MakeCStringNotFilled(size));
        buf.CopyTo(ret.get(), size);
        return PointerDatumToPod((Datum)ret.release());
    }
    default:
        TPAllocScope call;
        auto size = NDetails::UnpackUInt32(buf);
        std::unique_ptr<char[]> tmpBuf(new char[size]);
        buf.CopyTo(tmpBuf.get(), size);
        TStringBuf s{tmpBuf.get(), size};
        return NYql::NCommon::PgValueFromNativeBinary(s, type->GetTypeId());
    }
}

void EncodePresortPGValue(TPgType* type, const NUdf::TUnboxedValue& value, TVector<ui8>& output) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = DatumGetBool(ScalarDatumFromPod(value)) != 0;
        NDetail::EncodeBool<false>(output, x);
        break;
    }
    case INT2OID: {
        const auto x = DatumGetInt16(ScalarDatumFromPod(value));
        NDetail::EncodeSigned<i16, false>(output, x);
        break;
    }
    case INT4OID: {
        const auto x = DatumGetInt32(ScalarDatumFromPod(value));
        NDetail::EncodeSigned<i32, false>(output, x);
        break;
    }
    case INT8OID: {
        const auto x = DatumGetInt64(ScalarDatumFromPod(value));
        NDetail::EncodeSigned<i64, false>(output, x);
        break;
    }
    case FLOAT4OID: {
        const auto x = DatumGetFloat4(ScalarDatumFromPod(value));
        NDetail::EncodeFloating<float, false>(output, x);
        break;
    }
    case FLOAT8OID: {
        const auto x = DatumGetFloat8(ScalarDatumFromPod(value));
        NDetail::EncodeFloating<double, false>(output, x);
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        NDetail::EncodeString<false>(output, s);
        break;
    }
    case CSTRINGOID: {
        const auto x = (const char*)PointerDatumFromPod(value);
        NDetail::EncodeString<false>(output, x);
        break;
    }
    default:
        NYql::NCommon::PgValueToNativeBinaryImpl(value, type->GetTypeId(), true, [&output](TStringBuf b) {
            NDetail::EncodeString<false>(output, b);
        });
    }
}

NUdf::TUnboxedValue DecodePresortPGValue(TPgType* type, TStringBuf& input, TVector<ui8>& buffer) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = NDetail::DecodeBool<false>(input);
        return ScalarDatumToPod(BoolGetDatum(x));
    }
    case INT2OID: {
        const auto x = NDetail::DecodeSigned<i16, false>(input);
        return ScalarDatumToPod(Int16GetDatum(x));
    }
    case INT4OID: {
        const auto x = NDetail::DecodeSigned<i32, false>(input);
        return ScalarDatumToPod(Int32GetDatum(x));
    }
    case INT8OID: {
        const auto x = NDetail::DecodeSigned<i64, false>(input);
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        const auto x = NDetail::DecodeFloating<float, false>(input);
        return ScalarDatumToPod(Float4GetDatum(x));
    }
    case FLOAT8OID: {
        const auto x = NDetail::DecodeFloating<double, false>(input);
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        buffer.clear();
        const auto s = NDetail::DecodeString<false>(input, buffer);
        auto ret = MakeVar(s);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        buffer.clear();
        const auto s = NDetail::DecodeString<false>(input, buffer);
        auto ret = MakeCString(s);
        return PointerDatumToPod((Datum)ret);
    }
    default:
        buffer.clear();
        const auto s = NDetail::DecodeString<false>(input, buffer);
        return NYql::NCommon::PgValueFromNativeBinary(s, type->GetTypeId());
    }
}

void* PgInitializeContext(const std::string_view& contextType) {
    if (contextType == "Agg") {
        auto ctx = (AggState*)TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(AggState));
        Zero(*ctx);
        *(NodeTag*)ctx = T_AggState;
        ctx->curaggcontext = (ExprContext*)TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(ExprContext));
        Zero(*ctx->curaggcontext);
        ctx->curaggcontext->ecxt_per_tuple_memory = (MemoryContext)&((TMainContext*)TlsAllocState->MainContext)->Data;
        return ctx;
    } else if (contextType == "WinAgg") {
        auto ctx = (WindowAggState*)TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(WindowAggState));
        Zero(*ctx);
        *(NodeTag*)ctx = T_WindowAggState;
        ctx->curaggcontext = (MemoryContext)&((TMainContext*)TlsAllocState->MainContext)->Data;
        return ctx;
    } else {
        ythrow yexception() << "Unsupported context type: " << contextType;
    }
}

void PgDestroyContext(const std::string_view& contextType, void* ctx) {
    if (contextType == "Agg") {
        TWithDefaultMiniKQLAlloc::FreeWithSize(((AggState*)ctx)->curaggcontext, sizeof(ExprContext));
        TWithDefaultMiniKQLAlloc::FreeWithSize(ctx, sizeof(AggState));
    } else if (contextType == "WinAgg") {
        TWithDefaultMiniKQLAlloc::FreeWithSize(ctx, sizeof(WindowAggState));
    } else {
        Y_ABORT("Unsupported context type");
    }
}

template <bool PassByValue, bool IsArray>
class TPgHashBase {
public:
    TPgHashBase(const NYql::NPg::TTypeDesc& typeDesc)
        : TypeDesc(typeDesc)
    {
        auto hashProcId = TypeDesc.HashProcId;
        if constexpr (IsArray) {
            const auto& elemDesc = NYql::NPg::LookupType(TypeDesc.ElementTypeId);
            Y_ENSURE(elemDesc.HashProcId);

            hashProcId = NYql::NPg::LookupProc("hash_array", { 0, 0 }).ProcId;
        }

        Y_ENSURE(hashProcId);;
        Zero(FInfoHash);
        fmgr_info(hashProcId, &FInfoHash);
        Y_ENSURE(!FInfoHash.fn_retset);
        Y_ENSURE(FInfoHash.fn_addr);
        Y_ENSURE(FInfoHash.fn_nargs == 1);
    }

protected:
    const NYql::NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoHash;
};

template <bool PassByValue, bool IsArray>
class TPgHash : public TPgHashBase<PassByValue, IsArray>, public NUdf::IHash {
public:
    using TBase = TPgHashBase<PassByValue, IsArray>;

    TPgHash(const NYql::NPg::TTypeDesc& typeDesc)
        : TBase(typeDesc)
    {}

    ui64 Hash(NUdf::TUnboxedValuePod lhs) const override {
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoHash); // don't copy becase of IHash isn't threadsafe
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            return 0;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };

        auto x = this->FInfoHash.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetUInt32(x);
    }
};

template <bool PassByValue, bool IsArray>
class TPgHashItem : public TPgHashBase<PassByValue, IsArray>, public NUdf::TBlockItemHasherBase<TPgHashItem<PassByValue, IsArray>, true> {
public:
    using TBase = TPgHashBase<PassByValue, IsArray>;

    TPgHashItem(const NYql::NPg::TTypeDesc& typeDesc)
        : TBase(typeDesc)
    {}

    ui64 DoHash(NUdf::TBlockItem value) const {
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoHash); // don't copy becase of IHash isn't threadsafe
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(value) :
            PointerDatumFromItem(value), false };

        auto x = this->FInfoHash.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetUInt32(x);
    }
};

NUdf::IHash::TPtr MakePgHash(const NMiniKQL::TPgType* type) {
    const auto& typeDesc = NYql::NPg::LookupType(type->GetTypeId());
    if (typeDesc.PassByValue) {
        return new TPgHash<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgHash<false, true>(typeDesc);
    } else {
        return new TPgHash<false, false>(typeDesc);
    }
}

NUdf::IBlockItemHasher::TPtr MakePgItemHasher(ui32 typeId) {
    const auto& typeDesc = NYql::NPg::LookupType(typeId);
    if (typeDesc.PassByValue) {
        return new TPgHashItem<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgHashItem<false, true>(typeDesc);
    } else {
        return new TPgHashItem<false, false>(typeDesc);
    }
}

template <bool PassByValue, bool IsArray>
class TPgCompareBase {
public:
    TPgCompareBase(const NYql::NPg::TTypeDesc& typeDesc)
        : TypeDesc(typeDesc)
    {
        Zero(FInfoLess);
        Zero(FInfoCompare);
        Zero(FInfoEquals);

        auto lessProcId = TypeDesc.LessProcId;
        auto compareProcId = TypeDesc.CompareProcId;
        auto equalProcId = TypeDesc.EqualProcId;
        if constexpr (IsArray) {
            const auto& elemDesc = NYql::NPg::LookupType(TypeDesc.ElementTypeId);
            Y_ENSURE(elemDesc.CompareProcId);

            compareProcId = NYql::NPg::LookupProc("btarraycmp", { 0, 0 }).ProcId;
        } else {
            Y_ENSURE(lessProcId);
            Y_ENSURE(equalProcId);

            fmgr_info(lessProcId, &FInfoLess);
            Y_ENSURE(!FInfoLess.fn_retset);
            Y_ENSURE(FInfoLess.fn_addr);
            Y_ENSURE(FInfoLess.fn_nargs == 2);

            fmgr_info(equalProcId, &FInfoEquals);
            Y_ENSURE(!FInfoEquals.fn_retset);
            Y_ENSURE(FInfoEquals.fn_addr);
            Y_ENSURE(FInfoEquals.fn_nargs == 2);
        }

        Y_ENSURE(compareProcId);
        fmgr_info(compareProcId, &FInfoCompare);
        Y_ENSURE(!FInfoCompare.fn_retset);
        Y_ENSURE(FInfoCompare.fn_addr);
        Y_ENSURE(FInfoCompare.fn_nargs == 2);
    }

protected:
    const NYql::NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoLess, FInfoCompare, FInfoEquals;
};

template <bool PassByValue, bool IsArray>
class TPgCompare : public TPgCompareBase<PassByValue, IsArray>, public NUdf::ICompare {
public:
    using TBase = TPgCompareBase<PassByValue, IsArray>;

    TPgCompare(const NYql::NPg::TTypeDesc& typeDesc)
        : TBase(typeDesc)
    {}

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        if constexpr (IsArray) {
            return Compare(lhs, rhs) < 0;
        }

        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoLess); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            if (!rhs) {
                return false;
            }

            return true;
        }

        if (!rhs) {
            return false;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = this->FInfoLess.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoCompare); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            if (!rhs) {
                return 0;
            }

            return -1;
        }

        if (!rhs) {
            return 1;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = this->FInfoCompare.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(x);
    }
};

template <bool PassByValue, bool IsArray>
class TPgCompareItem : public TPgCompareBase<PassByValue, IsArray>, public NUdf::TBlockItemComparatorBase<TPgCompareItem<PassByValue, IsArray>, true> {
public:
    using TBase = TPgCompareBase<PassByValue, IsArray>;

    TPgCompareItem(const NYql::NPg::TTypeDesc& typeDesc)
        : TBase(typeDesc)
    {}

    i64 DoCompare(NUdf::TBlockItem lhs, NUdf::TBlockItem rhs) const {
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoCompare); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(lhs) :
            PointerDatumFromItem(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromItem(rhs) :
            PointerDatumFromItem(rhs), false };

        auto x = this->FInfoCompare.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(x);
    }

    bool DoEquals(NUdf::TBlockItem lhs, NUdf::TBlockItem rhs) const {
        if constexpr (IsArray) {
            return DoCompare(lhs, rhs) == 0;
        }

        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoEquals); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(lhs) :
            PointerDatumFromItem(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromItem(rhs) :
            PointerDatumFromItem(rhs), false };

        auto x = this->FInfoEquals.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }

    bool DoLess(NUdf::TBlockItem lhs, NUdf::TBlockItem rhs) const {
        if constexpr (IsArray) {
            return DoCompare(lhs, rhs) < 0;
        }

        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&this->FInfoLess); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(lhs) :
            PointerDatumFromItem(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromItem(rhs) :
            PointerDatumFromItem(rhs), false };

        auto x = this->FInfoLess.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }
};

NUdf::ICompare::TPtr MakePgCompare(const NMiniKQL::TPgType* type) {
    const auto& typeDesc = NYql::NPg::LookupType(type->GetTypeId());
    if (typeDesc.PassByValue) {
        return new TPgCompare<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgCompare<false, true>(typeDesc);
    } else {
        return new TPgCompare<false, false>(typeDesc);
    }
}

NUdf::IBlockItemComparator::TPtr MakePgItemComparator(ui32 typeId) {
    const auto& typeDesc = NYql::NPg::LookupType(typeId);
    if (typeDesc.PassByValue) {
        return new TPgCompareItem<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgCompareItem<false, true>(typeDesc);
    } else {
        return new TPgCompareItem<false, false>(typeDesc);
    }
}

template <bool PassByValue, bool IsArray>
class TPgEquate: public NUdf::IEquate {
public:
    TPgEquate(const NYql::NPg::TTypeDesc& typeDesc)
        : TypeDesc(typeDesc)
    {
        auto equalProcId = TypeDesc.EqualProcId;
        if constexpr (IsArray) {
            const auto& elemDesc = NYql::NPg::LookupType(TypeDesc.ElementTypeId);
            Y_ENSURE(elemDesc.CompareProcId);

            equalProcId = NYql::NPg::LookupProc("btarraycmp", { 0, 0 }).ProcId;
        }

        Y_ENSURE(equalProcId);

        Zero(FInfoEquate);
        fmgr_info(equalProcId, &FInfoEquate);
        Y_ENSURE(!FInfoEquate.fn_retset);
        Y_ENSURE(FInfoEquate.fn_addr);
        Y_ENSURE(FInfoEquate.fn_nargs == 2);
    }

    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoEquate); // don't copy becase of IEquate isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            if (!rhs) {
                return true;
            }

            return false;
        }

        if (!rhs) {
            return false;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = FInfoEquate.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        if constexpr (IsArray) {
            return DatumGetInt32(x) == 0;
        }

        return DatumGetBool(x);
    }

private:
    const NYql::NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoEquate;
};

NUdf::IEquate::TPtr MakePgEquate(const TPgType* type) {
    const auto& typeDesc = NYql::NPg::LookupType(type->GetTypeId());
    if (typeDesc.PassByValue) {
        return new TPgEquate<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgEquate<false, true>(typeDesc);
    } else {
        return new TPgEquate<false, false>(typeDesc);
    }
}

void* PgInitializeMainContext() {
    auto ctx = new TMainContext();
    MemoryContextCreate((MemoryContext)&ctx->Data,
        T_AllocSetContext,
        &MkqlMethods,
        nullptr,
        "mkql");
    MemoryContextCreate((MemoryContext)&ctx->ErrorData,
        T_AllocSetContext,
        &MkqlMethods,
        nullptr,
        "mkql-err");
    ctx->StartTimestamp = GetCurrentTimestamp();
    return ctx;
}

void PgDestroyMainContext(void* ctx) {
    delete (TMainContext*)ctx;
}

void PgAcquireThreadContext(void* ctx) {
    if (ctx) {
        pg_thread_init();
        auto main = (TMainContext*)ctx;
        main->PrevCurrentMemoryContext = CurrentMemoryContext;
        main->PrevErrorContext = ErrorContext;
        main->PrevCacheMemoryContext = CacheMemoryContext;
        SaveRecordCacheState(&main->PrevRecordCacheState);
        LoadRecordCacheState(&main->CurrentRecordCacheState);
        CurrentMemoryContext = CacheMemoryContext = (MemoryContext)&main->Data;
        ErrorContext = (MemoryContext)&main->ErrorData;
        SetParallelStartTimestamps(main->StartTimestamp, main->StartTimestamp);
        main->PrevStackBase = set_stack_base();
        yql_error_report_active = true;
    }
}

void PgReleaseThreadContext(void* ctx) {
    if (ctx) {
        auto main = (TMainContext*)ctx;
        CurrentMemoryContext = main->PrevCurrentMemoryContext;
        ErrorContext = main->PrevErrorContext;
        CacheMemoryContext = main->PrevCacheMemoryContext;
        SaveRecordCacheState(&main->CurrentRecordCacheState);
        LoadRecordCacheState(&main->PrevRecordCacheState);
        restore_stack_base(main->PrevStackBase);
        yql_error_report_active = false;
    }
}

extern "C" void yql_prepare_error(const char* msg) {
    auto ctx  = (TMainContext*)TlsAllocState->MainContext;
    ctx->LastError = msg;
}

extern "C" void yql_raise_error() {
    auto ctx  = (TMainContext*)TlsAllocState->MainContext;
    UdfTerminate(ctx->LastError.c_str());
}

} // namespace NMiniKQL
} // namespace NKikimr

namespace NYql {

class TPgBuilderImpl : public NUdf::IPgBuilder {
public:
     NUdf::TUnboxedValue ValueFromText(ui32 typeId, const NUdf::TStringRef& value, NUdf::TStringValue& error) const override {
        try {
            return NCommon::PgValueFromNativeText(static_cast<TStringBuf>(value), typeId);
        } catch (const std::exception& e) {
            error = NUdf::TStringValue(TStringBuf(e.what()));
        }
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue ValueFromBinary(ui32 typeId, const NUdf::TStringRef& value, NUdf::TStringValue& error) const override {
        try {
            return NCommon::PgValueFromNativeBinary(static_cast<TStringBuf>(value), typeId);
        } catch (const std::exception& e) {
            error = NUdf::TStringValue(TStringBuf(e.what()));
        }
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue ConvertFromPg(NUdf::TUnboxedValue source, ui32 sourceTypeId, const NUdf::TType* targetType) const override {
        auto t = static_cast<const NKikimr::NMiniKQL::TType*>(targetType);
        return ConvertFromPgValue(source, sourceTypeId, const_cast<NKikimr::NMiniKQL::TType*>(t));
    }

    NUdf::TUnboxedValue ConvertToPg(NUdf::TUnboxedValue source, const NUdf::TType* sourceType, ui32 targetTypeId) const override {
        auto t = static_cast<const NKikimr::NMiniKQL::TType*>(sourceType);
        return ConvertToPgValue(source, const_cast<NKikimr::NMiniKQL::TType*>(t), targetTypeId);
    }

    NUdf::TUnboxedValue NewString(i32 typeLen, ui32 targetTypeId, NUdf::TStringRef data) const override {
        return CreatePgString(typeLen, targetTypeId, data);
    }

    NUdf::TStringRef AsCStringBuffer(const NUdf::TUnboxedValue& value) const override {
        auto x = (const char*)PointerDatumFromPod(value);
        return { x, ui32(strlen(x) + 1)};
    }

    NUdf::TStringRef AsTextBuffer(const NUdf::TUnboxedValue& value) const override {
        auto x = (const text*)PointerDatumFromPod(value);
        return { (const char*)x, GetFullVarSize(x) };
    }

    NUdf::TUnboxedValue MakeCString(const char* value) const override {
        auto len = 1 + strlen(value);
        char* ret = (char*)palloc(len);
        memcpy(ret, value, len);
        return PointerDatumToPod((Datum)ret);
    }

    NUdf::TUnboxedValue MakeText(const char* value) const override {
        auto len = GetFullVarSize((const text*)value);
        char* ret = (char*)palloc(len);
        memcpy(ret, value, len);
        return PointerDatumToPod((Datum)ret);
    }

    NUdf::TStringRef AsFixedStringBuffer(const NUdf::TUnboxedValue& value, ui32 length) const override {
        auto x = (const char*)PointerDatumFromPod(value);
        return { x, length };
    }
};

std::unique_ptr<NUdf::IPgBuilder> CreatePgBuilder() {
    return std::make_unique<TPgBuilderImpl>();
}

} // namespace NYql

extern "C" {

void yql_canonize_float4(float4* x) {
    if (NYql::NeedCanonizeFp) {
        NYql::CanonizeFpBits<float>(x);
    }
}

extern void yql_canonize_float8(float8* x) {
    if (NYql::NeedCanonizeFp) {
        NYql::CanonizeFpBits<double>(x);
    }
}

void get_type_io_data(Oid typid,
    IOFuncSelector which_func,
    int16 *typlen,
    bool *typbyval,
    char *typalign,
    char *typdelim,
    Oid *typioparam,
    Oid *func) {
    const auto& typeDesc = NYql::NPg::LookupType(typid);
    *typlen = typeDesc.TypeLen;
    *typbyval = typeDesc.PassByValue;
    *typalign = typeDesc.TypeAlign;
    *typdelim = typeDesc.TypeDelim;
    *typioparam = NYql::MakeTypeIOParam(typeDesc);
    switch (which_func) {
    case IOFunc_input:
        *func = typeDesc.InFuncId;
        break;
    case IOFunc_output:
        *func = typeDesc.OutFuncId;
        break;
    case IOFunc_receive:
        *func = typeDesc.ReceiveFuncId;
        break;
    case IOFunc_send:
        *func = typeDesc.SendFuncId;
        break;
    }
}

} // extern "C"

namespace NKikimr::NPg {

constexpr char INTERNAL_TYPE_AND_MOD_SEPARATOR = ':';

class TPgTypeDescriptor
    : public NYql::NPg::TTypeDesc
{
public:
    explicit TPgTypeDescriptor(const NYql::NPg::TTypeDesc& desc)
        : NYql::NPg::TTypeDesc(desc)
    {
        if (TypeId == ArrayTypeId) {
            const auto& typeDesc = NYql::NPg::LookupType(ElementTypeId);
            YdbTypeName = TString("_pg") + typeDesc.Name;
            if (typeDesc.CompareProcId) {
                CompareProcId = NYql::NPg::LookupProc("btarraycmp", { 0, 0 }).ProcId;
            }
            if (typeDesc.HashProcId) {
                HashProcId = NYql::NPg::LookupProc("hash_array", { 0 }).ProcId;
            }
            if (typeDesc.ReceiveFuncId) {
                ReceiveFuncId = NYql::NPg::LookupProc("array_recv", { 0, 0, 0 }).ProcId;
            }
            if (typeDesc.SendFuncId) {
                SendFuncId = NYql::NPg::LookupProc("array_send", { 0 }).ProcId;
            }
            if (typeDesc.InFuncId) {
                InFuncId = NYql::NPg::LookupProc("array_in", { 0, 0, 0 }).ProcId;
            }
            if (typeDesc.OutFuncId) {
                OutFuncId = NYql::NPg::LookupProc("array_out", { 0 }).ProcId;
            }
            if (NYql::NPg::HasCast(ElementTypeId, ElementTypeId) && typeDesc.TypeModInFuncId) {
                NeedsCoercion = true;
                TypeModInFuncId = typeDesc.TypeModInFuncId;
            }
        } else {
            YdbTypeName = TString("pg") + desc.Name;
            StoredSize = TypeLen < 0 ? 0 : TypeLen;
            if (TypeId == NAMEOID) {
                StoredSize = 0; // store 'name' as usual string
            }
            if (NYql::NPg::HasCast(TypeId, TypeId) && TypeModInFuncId) {
                NeedsCoercion = true;
            }
        }
    }

    int Compare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datumL = 0, datumR = 0;
        Y_DEFER {
            if (!PassByValue) {
                if (datumL)
                    pfree((void*)datumL);
                if (datumR)
                    pfree((void*)datumR);
            }
        };

        datumL = Receive(dataL, sizeL);
        datumR = Receive(dataR, sizeR);
        FmgrInfo finfo;
        InitFunc(CompareProcId, &finfo, 2, 2);
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datumL, false };
        callInfo->args[1] = { datumR, false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(result);
    }

    ui64 Hash(const char* data, size_t size) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
        };
        datum = Receive(data, size);
        FmgrInfo finfo;
        InitFunc(HashProcId, &finfo, 1, 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetUInt32(result);
    }

    TConvertResult NativeBinaryFromNativeText(const TString& str) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        text* serialized = nullptr;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
            if (serialized) {
                pfree(serialized);
            }
        };
        PG_TRY();
        {
        {
            FmgrInfo finfo;
            InitFunc(InFuncId, &finfo, 1, 3);
            LOCAL_FCINFO(callInfo, 3);
            Zero(*callInfo);
            callInfo->flinfo = &finfo;
            callInfo->nargs = 3;
            callInfo->fncollation = DEFAULT_COLLATION_OID;
            callInfo->isnull = false;
            callInfo->args[0] = { (Datum)str.Data(), false };
            callInfo->args[1] = { ObjectIdGetDatum(NMiniKQL::MakeTypeIOParam(*this)), false };
            callInfo->args[2] = { Int32GetDatum(-1), false };

            datum = finfo.fn_addr(callInfo);
            Y_ENSURE(!callInfo->isnull);
        }
        FmgrInfo finfo;
        InitFunc(SendFuncId, &finfo, 1, 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };

        serialized = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return {TString(NMiniKQL::GetVarBuf(serialized)), {}};
    }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error while converting text to binary: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {"", errMsg};
        }
        PG_END_TRY();
    }

    TConvertResult NativeTextFromNativeBinary(const TStringBuf binary) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        char* str = nullptr;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
            if (str) {
                pfree(str);
            }
        };
        PG_TRY();
        {
        datum = Receive(binary.Data(), binary.Size());
        FmgrInfo finfo;
        InitFunc(OutFuncId, &finfo, 1, 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };

        str = (char*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return {TString(str), {}};
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error while converting binary to text: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {"", errMsg};
        }
        PG_END_TRY();
    }

    TTypeModResult ReadTypeMod(const TString& str) const {
        TVector<TString> params;
        ::Split(str, ",", params);

        if (params.size() > 2) {
            TStringBuilder errMsg;
            errMsg << "Error in 'typemodin' function: "
                << NYql::NPg::LookupProc(TypeModInFuncId).Name
                << ", reason: too many parameters";
            return {-1, errMsg};
        }

        TVector<Datum> dvalues;
        TVector<bool> dnulls;
        dnulls.resize(params.size(), false);
        dvalues.reserve(params.size());

        TString textNumberParam;
        if (TypeId == INTERVALOID || TypeId == INTERVALARRAYOID) {
            i32 typmod = -1;
            auto ok = NYql::ParsePgIntervalModifier(params[0], typmod);
            if (!ok) {
                TStringBuilder errMsg;
                errMsg << "Error in 'typemodin' function: "
                    << NYql::NPg::LookupProc(TypeModInFuncId).Name
                    << ", reason: invalid parameter '" << params[0]
                    << "' for type pginterval";
                return {-1, errMsg};
            }
            textNumberParam = Sprintf("%d", typmod);
            dvalues.push_back(PointerGetDatum(textNumberParam.data()));
            if (params.size() > 1) {
                dvalues.push_back(PointerGetDatum(params[1].data()));
            }
        } else {
            for (size_t i = 0; i < params.size(); ++i) {
                dvalues.push_back(PointerGetDatum(params[i].data()));
            }
        }

        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        ArrayType* paramsArray = nullptr;
        Y_DEFER {
            if (paramsArray) {
                pfree(paramsArray);
            }
        };
        PG_TRY();
        {
            int ndims = 0;
            int dims[MAXDIM];
            int lbs[MAXDIM];

            ndims = 1;
            dims[0] = params.size();
            lbs[0] = 1;

            const auto& cstringDesc = NYql::NPg::LookupType(CSTRINGOID);
            paramsArray = construct_md_array(dvalues.data(), dnulls.data(), ndims, dims, lbs,
                cstringDesc.TypeId,
                cstringDesc.TypeLen,
                cstringDesc.PassByValue,
                cstringDesc.TypeAlign);

            FmgrInfo finfo;
            InitFunc(TypeModInFuncId, &finfo, 1, 1);
            LOCAL_FCINFO(callInfo, 1);
            Zero(*callInfo);
            callInfo->flinfo = &finfo;
            callInfo->nargs = 1;
            callInfo->fncollation = DEFAULT_COLLATION_OID;
            callInfo->isnull = false;
            callInfo->args[0] = { PointerGetDatum(paramsArray), false };

            auto result = finfo.fn_addr(callInfo);
            Y_ENSURE(!callInfo->isnull);
            return {DatumGetInt32(result), {}};
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in 'typemodin' function: "
                << NYql::NPg::LookupProc(TypeModInFuncId).Name
                << ", reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {-1, errMsg};
        }
        PG_END_TRY();
    }

    TMaybe<TString> Validate(const TStringBuf binary) {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
        };
        PG_TRY();
        {
        datum = Receive(binary.Data(), binary.Size());
        return {};
    }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in 'recv' function: "
                << NYql::NPg::LookupProc(ReceiveFuncId).Name
                << ", reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return errMsg;
        }
        PG_END_TRY();
    }

    TCoerceResult Coerce(const TStringBuf binary, i32 typmod) {
        return Coerce(true, binary, 0, typmod);
    }

    TCoerceResult Coerce(const NUdf::TUnboxedValuePod& value, i32 typmod) {
        Datum datum = PassByValue ?
            NMiniKQL::ScalarDatumFromPod(value) :
            NMiniKQL::PointerDatumFromPod(value);

        return Coerce(false, {}, datum, typmod);
    }

private:
    TCoerceResult Coerce(bool isSourceBinary, const TStringBuf binary, Datum datum, i32 typmod) {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;

        Datum datumCasted = 0;
        TVector<Datum> elems;
        TVector<bool> nulls;
        TVector<Datum> castedElements;
        bool passByValueElem = false;
        text* serialized = nullptr;
        Y_DEFER {
            if (!PassByValue) {
                if (datum && isSourceBinary) {
                    pfree((void*)datum);
                }
                if (datumCasted) {
                    pfree((void*)datumCasted);
                }
            }
            if (IsArray() && !passByValueElem) {
                for (ui32 i = 0; i < castedElements.size(); ++i) {
                    pfree((void*)castedElements[i]);
                }
            }
            if (serialized) {
                pfree(serialized);
            }
        };
        PG_TRY();
        {
            if (isSourceBinary) {
                datum = Receive(binary.Data(), binary.Size());
            }

            if (IsArray()) {
                const auto& typeDesc = NYql::NPg::LookupType(ElementTypeId);
                passByValueElem = typeDesc.PassByValue;

                auto arr = (ArrayType*)DatumGetPointer(datum);
                auto ndim = ARR_NDIM(arr);
                auto dims = ARR_DIMS(arr);
                auto lb = ARR_LBOUND(arr);
                auto nitems = ArrayGetNItems(ndim, dims);

                elems.resize(nitems);
                nulls.resize(nitems);
                castedElements.reserve(nitems);

                array_iter iter;
                array_iter_setup(&iter, (AnyArrayType*)arr);
                for (ui32 i = 0; i < nitems; ++i) {
                    bool isNull;
                    auto datum = array_iter_next(&iter, &isNull, i,
                        typeDesc.TypeLen, typeDesc.PassByValue, typeDesc.TypeAlign);
                    if (isNull) {
                        elems[i] = 0;
                        nulls[i] = true;
                        continue;
                    }
                    elems[i] = CoerceOne(ElementTypeId, datum, typmod);
                    nulls[i] = false;
                    if (elems[i] != datum) {
                        castedElements.push_back(elems[i]);
                    }
                }

                if (!castedElements.empty()) {
                    auto newArray = construct_md_array(elems.data(), nulls.data(), ndim, dims, lb,
                        typeDesc.TypeId, typeDesc.TypeLen, typeDesc.PassByValue, typeDesc.TypeAlign);
                    datumCasted = PointerGetDatum(newArray);
                }
            } else {
                datumCasted = CoerceOne(TypeId, datum, typmod);
                if (datumCasted == datum) {
                    datumCasted = 0;
                }
            }

            if (!datumCasted && isSourceBinary) {
                return {{}, {}};
            } else {
                FmgrInfo finfo;
                InitFunc(SendFuncId, &finfo, 1, 1);
                LOCAL_FCINFO(callInfo, 1);
                Zero(*callInfo);
                callInfo->flinfo = &finfo;
                callInfo->nargs = 1;
                callInfo->fncollation = DEFAULT_COLLATION_OID;
                callInfo->isnull = false;
                callInfo->args[0] = { datumCasted ? datumCasted : datum, false };

                serialized = (text*)finfo.fn_addr(callInfo);
                Y_ENSURE(!callInfo->isnull);
                return {TString(NMiniKQL::GetVarBuf(serialized)), {}};
            }
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error while coercing value, reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {{}, errMsg};
        }
        PG_END_TRY();
    }

    Datum CoerceOne(ui32 typeId, Datum datum, i32 typmod) const {
        const auto& cast = NYql::NPg::LookupCast(typeId, typeId);

        FmgrInfo finfo;
        InitFunc(cast.FunctionId, &finfo, 2, 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };
        callInfo->args[1] = { Int32GetDatum(typmod), false };
        callInfo->args[2] = { BoolGetDatum(false), false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return result;
    }

    Datum Receive(const char* data, size_t size) const {
        StringInfoData stringInfo;
        stringInfo.data = (char*)data;
        stringInfo.len = size;
        stringInfo.maxlen = size;
        stringInfo.cursor = 0;

        FmgrInfo finfo;
        InitFunc(ReceiveFuncId, &finfo, 1, 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)&stringInfo, false };
        callInfo->args[1] = { ObjectIdGetDatum(NMiniKQL::MakeTypeIOParam(*this)), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return result;
    }

    bool IsArray() {
        return TypeId == ArrayTypeId;
    }

    static inline void InitFunc(ui32 funcId, FmgrInfo* info, ui32 argCountMin, ui32 argCountMax) {
        Zero(*info);
        Y_ENSURE(funcId);
        fmgr_info(funcId, info);
        Y_ENSURE(info->fn_addr);
        Y_ENSURE(info->fn_nargs >= argCountMin && info->fn_nargs <= argCountMax);
    }

public:
    TString YdbTypeName;
    ui32 StoredSize = 0; // size in local db, 0 for variable size
    bool NeedsCoercion = false;
};

class TPgTypeDescriptors {
public:
    static const TPgTypeDescriptors& Instance() {
        return *Singleton<TPgTypeDescriptors>();
    }

    TPgTypeDescriptors() {
        auto initType = [this] (ui32 pgTypeId, const NYql::NPg::TTypeDesc& type) {
            this->InitType(pgTypeId, type);
        };
        NYql::NPg::EnumTypes(initType);
    }

    const TPgTypeDescriptor* Find(ui32 pgTypeId) const {
        return PgTypeDescriptors.FindPtr(pgTypeId);
    }

    const TPgTypeDescriptor* Find(const TStringBuf name) const {
        auto* id = ByName.FindPtr(name);
        if (id) {
            return Find(*id);
        }
        return {};
    }

private:
    void InitType(ui32 pgTypeId, const NYql::NPg::TTypeDesc& type) {
        auto desc = TPgTypeDescriptor(type);
        ByName[desc.YdbTypeName] = pgTypeId;
        PgTypeDescriptors.emplace(pgTypeId, desc);
    }

private:
    THashMap<ui32, TPgTypeDescriptor> PgTypeDescriptors;
    THashMap<TString, ui32> ByName;
};

ui32 PgTypeIdFromTypeDesc(void* typeDesc) {
    if (!typeDesc) {
        return 0;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->TypeId;
}

void* TypeDescFromPgTypeId(ui32 pgTypeId) {
    if (!pgTypeId) {
        return {};
    }
    return (void*)TPgTypeDescriptors::Instance().Find(pgTypeId);
}

TString PgTypeNameFromTypeDesc(void* typeDesc, const TString& typeMod) {
    if (!typeDesc) {
        return "";
    }
    auto* pgTypeDesc = static_cast<TPgTypeDescriptor*>(typeDesc);
    if (typeMod.empty()) {
        return pgTypeDesc->YdbTypeName;
    }
    return pgTypeDesc->YdbTypeName + INTERNAL_TYPE_AND_MOD_SEPARATOR + typeMod;
}

void* TypeDescFromPgTypeName(const TStringBuf name) {
    auto space = name.find_first_of(INTERNAL_TYPE_AND_MOD_SEPARATOR);
    if (space != TStringBuf::npos) {
        return (void*)TPgTypeDescriptors::Instance().Find(name.substr(0, space));
    }
    return (void*)TPgTypeDescriptors::Instance().Find(name);
}

TString TypeModFromPgTypeName(const TStringBuf name) {
    auto space = name.find_first_of(INTERNAL_TYPE_AND_MOD_SEPARATOR);
    if (space != TStringBuf::npos) {
        return TString(name.substr(space + 1));
    }
    return {};
}

bool TypeDescIsComparable(void* typeDesc) {
    if (!typeDesc) {
        return false;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->CompareProcId != 0;
}

i32 TypeDescGetTypeLen(void* typeDesc) {
    if (!typeDesc) {
        return 0;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->TypeLen;
}

ui32 TypeDescGetStoredSize(void* typeDesc) {
    if (!typeDesc) {
        return 0;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->StoredSize;
}

bool TypeDescNeedsCoercion(void* typeDesc) {
    if (!typeDesc) {
        return false;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->NeedsCoercion;
}

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, void* typeDesc) {
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Compare(dataL, sizeL, dataR, sizeR);
}

ui64 PgNativeBinaryHash(const char* data, size_t size, void* typeDesc) {
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Hash(data, size);
}

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, void* typeDesc) {
    if (!typeDesc) {
        return {-1, "invalid type descriptor"};
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->ReadTypeMod(str);
}

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, void* typeDesc) {
    if (!typeDesc) {
        return "invalid type descriptor";
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Validate(binary);
}

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, void* typeDesc, i32 typmod) {
    if (!typeDesc) {
        return {{}, "invalid type descriptor"};
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Coerce(binary, typmod);
}

TConvertResult PgNativeBinaryFromNativeText(const TString& str, void* typeDesc) {
    if (!typeDesc) {
        return {{}, "invalid type descriptor"};
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->NativeBinaryFromNativeText(str);
}

TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId) {
    return PgNativeBinaryFromNativeText(str, TypeDescFromPgTypeId(pgTypeId));
}

TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, void* typeDesc) {
    if (!typeDesc) {
        return {{}, "invalid type descriptor"};
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->NativeTextFromNativeBinary(binary);
}

TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, ui32 pgTypeId) {
    return PgNativeTextFromNativeBinary(binary, TypeDescFromPgTypeId(pgTypeId));
}

} // namespace NKikimr::NPg

namespace NYql::NCommon {

TString PgValueCoerce(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId, i32 typMod, TMaybe<TString>* error) {
    auto* typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(pgTypeId);
    if (!typeDesc) {
        if (error) {
            *error = "invalid type descriptor";
        }
        return {};
    }
    auto result = static_cast<NKikimr::NPg::TPgTypeDescriptor*>(typeDesc)->Coerce(value, typMod);
    if (result.Error) {
        if (error) {
            *error = result.Error;
        }
        return {};
    }
    return *result.NewValue;
}

} // namespace NYql::NCommon
