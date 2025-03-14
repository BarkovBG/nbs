#pragma once
#include <contrib/ydb/library/yql/ast/yql_errors.h>
#include <util/generic/hash.h>
#include <contrib/ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {
namespace NFastCheck {

struct TOptions {
   bool IsSql = true;
   bool ParseOnly = false;
   THashMap<TString, TString> ClusterMapping;
   ui16 SyntaxVersion = 1;
   bool IsLibrary = false;
   THashMap<TString, TString> SqlLibs = {}; // mapping file name => SQL
};

bool CheckProgram(const TString& program, const TOptions& options, TIssues& errors);

}
}
