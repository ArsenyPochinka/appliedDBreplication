package com.applied.replication.receiver.apply

import org.springframework.stereotype.Component

/**
 * Builds SQL scalar/array expressions that read fields from the bound JSON `:payload` parameter.
 */
@Component
class ReplicationPayloadJsonSqlExpressions {

    /**
     * Maps one JSON field from the `:payload` bind variable to a SQL scalar/array expression.
     */
    fun jsonbToSqlExpr(column: ReplicationColumnMeta): String {
        ReplicationSqlGuards.requireSafeColumnName(column.name)
        val p = "CAST(:payload AS jsonb)"
        val f = column.name
        return when {
            column.dataType.equals("ARRAY", ignoreCase = true) ->
                when (column.udtName) {
                    "_text" -> "ARRAY(SELECT jsonb_array_elements_text($p -> '$f'))::text[]"
                    else -> throw IllegalStateException(
                        "Unsupported Postgres array type '${column.udtName}' for column '$f'. Extend jsonbToSqlExpr()."
                    )
                }
            column.dataType.equals("jsonb", ignoreCase = true) || column.udtName.equals("jsonb", ignoreCase = true) ->
                "($p -> '$f')::jsonb"
            column.dataType.equals("json", ignoreCase = true) || column.udtName.equals("json", ignoreCase = true) ->
                "($p -> '$f')::json"
            else -> {
                val cast = pgTextJsonToScalarCast(column)
                "($p->>'$f')::$cast"
            }
        }
    }

    private fun pgTextJsonToScalarCast(column: ReplicationColumnMeta): String {
        val dt = column.dataType.lowercase()
        return when (dt) {
            "smallint", "integer", "bigint", "numeric", "decimal", "real",
            "double precision", "boolean", "date", "uuid",
            "time without time zone", "time with time zone",
            "timestamp without time zone", "timestamp with time zone" -> dt
            "character varying" -> "varchar"
            "character" -> "char"
            "text" -> "text"
            "USER-DEFINED" -> when (column.udtName) {
                "int2" -> "smallint"
                "int4" -> "integer"
                "int8" -> "bigint"
                "float4" -> "real"
                "float8" -> "double precision"
                "bool" -> "boolean"
                "numeric" -> "numeric"
                "timestamptz" -> "timestamptz"
                "timestamp" -> "timestamp"
                else -> "text"
            }
            else -> when (column.udtName) {
                "int2" -> "smallint"
                "int4" -> "integer"
                "int8" -> "bigint"
                "float4" -> "real"
                "float8" -> "double precision"
                "bool" -> "boolean"
                "numeric" -> "numeric"
                "timestamptz" -> "timestamptz"
                "timestamp" -> "timestamp"
                "date" -> "date"
                "text", "varchar", "bpchar" -> "text"
                else -> "text"
            }
        }
    }
}
