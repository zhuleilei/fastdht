#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <time.h>
#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "fastdht_client.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// Every user visible function must have an entry in fastdht_client_functions[].
	function_entry fastdht_client_functions[] = {
		ZEND_FE(fastdht_client_set_const, NULL)
		{NULL, NULL, NULL}  /* Must be the last line */
	};

// {{{ fastdht_client_module_entry 
zend_module_entry fastdht_client_module_entry = {
	STANDARD_MODULE_HEADER,
	"fastdht_client",
	fastdht_client_functions,
	PHP_MINIT(fastdht_client),
	PHP_MSHUTDOWN(fastdht_client),
	NULL,//PHP_RINIT(fastdht_client),
	NULL,//PHP_RSHUTDOWN(fastdht_client),
	PHP_MINFO(fastdht_client),
	"$Revision: 0.1 $", 
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_FASTDHT_CLIENT
	ZEND_GET_MODULE(fastdht_client)
#endif

ZEND_FUNCTION(fastdht_client_set_const){

	//zend_hash_update(EG(active_symbol_table),"foo_array",sizeof("foo_array"),&foo_array,sizeof(zval *),NULL);
	//zend_hash_update(&EG(symbol_table),"foo_array",sizeof("foo_array"),&foo_array,sizeof(zval *),NULL);

	/* introduce this variable by the name "new_variable_name" into the symbol table */
	//ZEND_SET_SYMBOL(&EG(symbol_table), "new_variable_name", new_variable); 
}

// {{{ PHP_MINIT_FUNCTION
PHP_MINIT_FUNCTION(fastdht_client)
{
	REGISTER_STRING_CONSTANT("FASTDHT_CONST", "test", CONST_CS|CONST_PERSISTENT);

	return SUCCESS;
}
// }}} 

// {{{ PHP_MSHUTDOWN_FUNCTION
PHP_MSHUTDOWN_FUNCTION(fastdht_client)
{
	fprintf(stderr, "module shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}
// }}}

//
// {{{ PHP_RINIT_FUNCTION
PHP_RINIT_FUNCTION(fastdht_client)
{
	return SUCCESS;
}
// }}} 

// {{{ PHP_RSHUTDOWN_FUNCTION
PHP_RSHUTDOWN_FUNCTION(fastdht_client)
{
	fprintf(stderr, "request shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}
// }}}

// {{{ PHP_MINFO_FUNCTION
PHP_MINFO_FUNCTION(fastdht_client)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "fastdht_client support", "enabled");
	php_info_print_table_end();

}
// }}} 
//}}}
