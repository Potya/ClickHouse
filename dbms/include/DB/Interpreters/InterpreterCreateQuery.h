#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Позволяет создать новую таблицу, или создать объект уже существующей таблицы, или создать БД, или создать объект уже существующей БД
  */
class InterpreterCreateQuery
{
public:
	InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_);
	
	/** В случае таблицы: добавляет созданную таблицу в контекст, а также возвращает её.
	  * В случае БД: добавляет созданную БД в контекст и возвращает NULL.
	  */
	StoragePtr execute();

private:
	ASTPtr query_ptr;
	Context context;
};


}
