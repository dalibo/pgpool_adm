/*-------------------------------------------------------------------------
 *
 * pgpool_adm.c
 *
 *
 * Copyright (c) 2002-2011, PostgreSQL Global Development Group
 *
 * Author: Jehan-Guillaume (ioguix) de Rorthais <jgdr@dalibo.com>
 *
 * IDENTIFICATION
 *	  contrib/pgpool_adm/pgpool_adm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"

#include <unistd.h>

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpcp_ext.h"
//#include "pcp.h"

PG_MODULE_MAGIC;

Datum _pcp_node_info(PG_FUNCTION_ARGS);
Datum _pcp_pool_status(PG_FUNCTION_ARGS);
Datum _pcp_node_count(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(_pcp_node_info);
PG_FUNCTION_INFO_V1(_pcp_pool_status);
PG_FUNCTION_INFO_V1(_pcp_node_count);

/**
 * nodeID: the node id to get info from
 * host_or_srv: server name or ip address of the pgpool server
 * timeout: timeout
 * port: pcp port number
 * user: user to connect with
 * pass: password
 **/
Datum
_pcp_node_info(PG_FUNCTION_ARGS)
{
	int16  nodeID = PG_GETARG_INT16(0);
	char * host_or_srv = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int16 timeout = -1;
	int16 port = -1;
	char * user = NULL;
	char * pass = NULL;
	Oid userid = GetUserId();

	BackendInfo * backend_info = NULL;
	Datum values[4]; /* values to build the returned tuple from */
	bool nulls[] = {false, false, false, false};
	TupleDesc tupledesc;
	HeapTuple tuple;

	if (PG_NARGS() == 6)
	{
		timeout = PG_GETARG_INT16(2);
		port = PG_GETARG_INT16(3);
		user = text_to_cstring(PG_GETARG_TEXT_PP(4));
		pass = text_to_cstring(PG_GETARG_TEXT_PP(5));
	}
	else if (PG_NARGS() == 2)
	{

		/* raise an error if given foreign server doesn't exists */
		ForeignServer * foreign_server = GetForeignServerByName(host_or_srv, false);
		UserMapping * user_mapping;
		ListCell * cell;

		/* raise an error if the current user isn't mapped with the given foreign server */
		user_mapping = GetUserMapping(userid, foreign_server->serverid);

		foreach(cell, foreign_server->options)
		{
			DefElem * def = lfirst(cell);

			if (strcmp(def->defname, "host") == 0)
			{
				host_or_srv = pstrdup(strVal(def->arg));
			}
			else if (strcmp(def->defname, "port") == 0)
			{
				port = atoi(strVal(def->arg));
			}
			else if (strcmp(def->defname, "timeout") == 0)
			{
				timeout = atoi(strVal(def->arg));
			}
		}

		foreach(cell, user_mapping->options)
		{
			DefElem * def = lfirst(cell);

			if (strcmp(def->defname, "user") == 0)
			{
				user = pstrdup(strVal(def->arg));
			}
			else if (strcmp(def->defname, "password") == 0)
			{
				pass = pstrdup(strVal(def->arg));
			}
		}
	}
	else
	{
		ereport(ERROR, (0, errmsg("Wrong number of argument.")));
	}

	/**
	 * basic checks for validity of parameters
	 **/

	if (nodeID < 0 || nodeID >= MAX_NUM_BACKENDS)
		ereport(ERROR, (0, errmsg("NodeID is out of range.")));

	if (timeout < 0)
		ereport(ERROR, (0, errmsg("Timeout is out of range.")));

	if (port < 0 || port > 65535)
		ereport(ERROR, (0, errmsg("PCP port out of range.")));

	if (! user)
		ereport(ERROR, (0, errmsg("No user given.")));

	if (! pass)
		ereport(ERROR, (0, errmsg("No password given.")));

	/**
	 * Construct a tuple descriptor for the result rows.
	 **/
	tupledesc = CreateTemplateTupleDesc(4, false);
	TupleDescInitEntry(tupledesc, (AttrNumber) 1, "hostname", TEXTOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 2, "port", INT4OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 3, "status", TEXTOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 4, "weight", FLOAT4OID, -1, 0);
	tupledesc = BlessTupleDesc(tupledesc);

	/**
	 * PCP session
	 **/
	pcp_set_timeout(timeout);

	if (pcp_connect(host_or_srv, port, user, pass))
	{
		ereport(ERROR,(0, errmsg("Cannot connect to PCP server.")));
	}

	if ((backend_info = pcp_node_info(nodeID)) == NULL)
	{
		pcp_disconnect();
		ereport(ERROR,(0, errmsg("Cannot get node information.")));
	}

	/* set values */
	values[0] = CStringGetTextDatum(backend_info->backend_hostname);
	nulls[0] = false;
	values[1] = Int16GetDatum(backend_info->backend_port);
	nulls[1] = false;
	switch (backend_info->backend_status)
	{
		case CON_UNUSED:
			values[2] = CStringGetTextDatum("Connection unused");
			break;
		case CON_CONNECT_WAIT:
			values[2] = CStringGetTextDatum("Waiting for connection to start");
			break;
		case CON_UP:
			values[2] = CStringGetTextDatum("Connection in use");
			break;
		case CON_DOWN:
			values[2] = CStringGetTextDatum("Disconnected");
			break;
	}
	nulls[2] = false;
	values[3] = Float8GetDatum(backend_info->backend_weight/RAND_MAX);
	nulls[3] = false;

	free(backend_info);
	pcp_disconnect();

	/* build and return the tuple */
	tuple = heap_form_tuple(tupledesc, values, nulls);

	ReleaseTupleDesc(tupledesc);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/**
 * host_or_srv: server name or ip address of the pgpool server
 * timeout: timeout
 * port: pcp port number
 * user: user to connect with
 * pass: password
 **/
Datum
_pcp_pool_status(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext;
	FuncCallContext *funcctx;
	Oid userid = GetUserId();
	POOL_REPORT_CONFIG *status;
	int32 nrows;
	int32 call_cntr;
	int32 max_calls;
	AttInMetadata *attinmeta;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupdesc;
		char * host_or_srv = text_to_cstring(PG_GETARG_TEXT_PP(0));
		int16 timeout = -1;
		int16 port = -1;
		char * user = NULL;
		char * pass = NULL;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (PG_NARGS() == 5)
		{
			timeout = PG_GETARG_INT16(1);
			port = PG_GETARG_INT16(2);
			user = text_to_cstring(PG_GETARG_TEXT_PP(3));
			pass = text_to_cstring(PG_GETARG_TEXT_PP(4));
		}
		else if (PG_NARGS() == 1)
		{

			/* raise an error if given foreign server doesn't exists */
			ForeignServer * foreign_server = GetForeignServerByName(host_or_srv, false);
			UserMapping * user_mapping;
			ListCell * cell;

			/* raise an error if the current user isn't mapped with the given foreign server */
			user_mapping = GetUserMapping(userid, foreign_server->serverid);

			foreach(cell, foreign_server->options)
			{
				DefElem * def = lfirst(cell);

				if (strcmp(def->defname, "host") == 0)
				{
					host_or_srv = pstrdup(strVal(def->arg));
				}
				else if (strcmp(def->defname, "port") == 0)
				{
					port = atoi(strVal(def->arg));
				}
				else if (strcmp(def->defname, "timeout") == 0)
				{
					timeout = atoi(strVal(def->arg));
				}
			}

			foreach(cell, user_mapping->options)
			{
				DefElem * def = lfirst(cell);

				if (strcmp(def->defname, "user") == 0)
				{
					user = pstrdup(strVal(def->arg));
				}
				else if (strcmp(def->defname, "password") == 0)
				{
					pass = pstrdup(strVal(def->arg));
				}
			}
		}
		else
		{
			MemoryContextSwitchTo(oldcontext);
			ereport(ERROR, (0, errmsg("Wrong number of argument.")));
		}

		/**
		 * basic checks for validity of parameters
		 **/

		if (timeout < 0) {
			MemoryContextSwitchTo(oldcontext);
			ereport(ERROR, (0, errmsg("Timeout is out of range.")));
		}

		if (port < 0 || port > 65535) {
			MemoryContextSwitchTo(oldcontext);
			ereport(ERROR, (0, errmsg("PCP port out of range.")));
		}

		if (! user) {
			MemoryContextSwitchTo(oldcontext);
			ereport(ERROR, (0, errmsg("No user given.")));
		}

		if (! pass) {
			MemoryContextSwitchTo(oldcontext);
			ereport(ERROR, (0, errmsg("No password given.")));
		}

		/* get configuration and status */
		/**
		 * PCP session
		 **/
		pcp_set_timeout(timeout);

		if (pcp_connect(host_or_srv, port, user, pass))
		{
			ereport(ERROR,(0, errmsg("Cannot connect to PCP server.")));
		}

		if ((status = pcp_pool_status(&nrows)) == NULL)
		{
			pcp_disconnect();
			ereport(ERROR,(0, errmsg("Cannot pool status information.")));
		}

		pcp_disconnect();

		/* Construct a tuple descriptor for the result rows */
		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "item", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "value", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "description", TEXTOID, -1, 0);
		//tupdesc = BlessTupleDesc(tupdesc);

		/*
		 * Generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		if ((status != NULL) && (nrows > 0))
		{
			funcctx->max_calls = nrows;

			/* got results, keep track of them */
			funcctx->user_fctx = status;
		}
		else
		{
			/* fast track when no results */
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	/* initialize per-call variables */
	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;

	status = (POOL_REPORT_CONFIG*) funcctx->user_fctx;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)	/* executed while there is more left to send */
	{
		char * values[3];
		HeapTuple tuple;
		Datum result;

		values[0] = pstrdup(status[call_cntr].name);
		values[1] = pstrdup(status[call_cntr].value);
		values[2] = pstrdup(status[call_cntr].desc);

		/* build the tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}

	free(status);
}

/**
 * nodeID: the node id to get info from
 * host_or_srv: server name or ip address of the pgpool server
 * timeout: timeout
 * port: pcp port number
 * user: user to connect with
 * pass: password
 **/
Datum
_pcp_node_count(PG_FUNCTION_ARGS)
{
	char * host_or_srv = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int16 timeout = -1;
	int16 port = -1;
	char * user = NULL;
	char * pass = NULL;

	int16 node_count = 0;

	if (PG_NARGS() == 5)
	{
		timeout = PG_GETARG_INT16(1);
		port = PG_GETARG_INT16(2);
		user = text_to_cstring(PG_GETARG_TEXT_PP(3));
		pass = text_to_cstring(PG_GETARG_TEXT_PP(4));
	}
	else if (PG_NARGS() == 1)
	{
		Oid userid = GetUserId();

		/* raise an error if given foreign server doesn't exists */
		ForeignServer * foreign_server = GetForeignServerByName(host_or_srv, false);
		UserMapping * user_mapping;
		ListCell * cell;

		/* raise an error if the current user isn't mapped with the given foreign server */
		user_mapping = GetUserMapping(userid, foreign_server->serverid);

		foreach(cell, foreign_server->options)
		{
			DefElem * def = lfirst(cell);

			if (strcmp(def->defname, "host") == 0)
			{
				host_or_srv = pstrdup(strVal(def->arg));
			}
			else if (strcmp(def->defname, "port") == 0)
			{
				port = atoi(strVal(def->arg));
			}
			else if (strcmp(def->defname, "timeout") == 0)
			{
				timeout = atoi(strVal(def->arg));
			}
		}

		foreach(cell, user_mapping->options)
		{
			DefElem * def = lfirst(cell);

			if (strcmp(def->defname, "user") == 0)
			{
				user = pstrdup(strVal(def->arg));
			}
			else if (strcmp(def->defname, "password") == 0)
			{
				pass = pstrdup(strVal(def->arg));
			}
		}
	}
	else
	{
		ereport(ERROR, (0, errmsg("Wrong number of argument.")));
	}

	/**
	 * basic checks for validity of parameters
	 **/
	if (timeout < 0)
		ereport(ERROR, (0, errmsg("Timeout is out of range.")));

	if (port < 0 || port > 65535)
		ereport(ERROR, (0, errmsg("PCP port out of range.")));

	if (! user)
		ereport(ERROR, (0, errmsg("No user given.")));

	if (! pass)
		ereport(ERROR, (0, errmsg("No password given.")));

	/**
	 * PCP session
	 **/
	pcp_set_timeout(timeout);

	if (pcp_connect(host_or_srv, port, user, pass))
	{
		ereport(ERROR,(0, errmsg("Cannot connect to PCP server.")));
	}

	if ((node_count = pcp_node_count()) == -1)
	{
		pcp_disconnect();
		ereport(ERROR,(0, errmsg("Cannot get node count.")));
	}

	pcp_disconnect();

	PG_RETURN_INT16(node_count);
}
