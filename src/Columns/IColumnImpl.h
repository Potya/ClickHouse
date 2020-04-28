/**
  * This file implements template methods of IColumn that depend on other types
  * we don't want to include.
  * Currently, this is only the scatterImpl method that depends on PODArray
  * implementation.
  */

#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

template <typename Derived>
std::vector<IColumn::MutablePtr> IColumn::scatterImpl(ColumnIndex num_columns, const Selector & selector) const
{
    size_t num_rows = size();

    if (num_rows != selector.size())
        throw Exception(
                "Size of selector: " + std::to_string(selector.size()) + " doesn't match size of column: " + std::to_string(num_rows),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<MutablePtr> columns(num_columns);
    for (auto & column : columns)
        column = cloneEmpty();

    {
        size_t reserve_size = num_rows * 1.1 / num_columns;    /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<Derived &>(*columns[selector[i]]).insertFrom(*this, i);

    return columns;
}

template <typename Derived>
void IColumn::scatterImplInplace(ColumnIndex num_columns, const Selector & selector, std::vector<ColumnPtr> & res_columns) const
{
    size_t num_rows = size();

    if (num_rows != selector.size())
        throw Exception(
            "Size of selector: " + std::to_string(selector.size()) + " doesn't match size of column: " + std::to_string(num_rows),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    res_columns.resize(num_columns);
    std::vector<IColumn::MutablePtr> mut_columns;
    mut_columns.reserve(num_columns);

    for (auto & column : res_columns)
    {
        if (column && column->use_count() == 1)
        {
            mut_columns.emplace_back((*std::move(column)).mutate());
            mut_columns.back()->clear();
        }
        else
            mut_columns.emplace_back(cloneEmpty());
    }

    {
        size_t reserve_size = num_rows * 1.1 / num_columns;    /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : mut_columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<Derived &>(*mut_columns[selector[i]]).insertFrom(*this, i);

    for (size_t col = 0; col < num_columns; ++col)
        res_columns[col] = std::move(mut_columns[col]);
}

}
