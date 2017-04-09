
/*
 * Copyright (C) xianliang.li
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_http_dfs.h>


typedef struct {
	ngx_msec_t	 			  timeout;
	ngx_http_upstream_conf_t  upstream;
} ngx_http_dfst_loc_conf_t;


typedef struct ngx_http_dfst_ctx_s  ngx_http_dfst_ctx_t;

typedef void (*ngx_http_dfst_handler_pt) (ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx);

struct ngx_http_dfst_ctx_s {
	ngx_udp_connection_t      	   uc;

	//ngx_buf_t  			  	 *request;
	ngx_http_dfs_request_header_t  request_header;
	u_short					  	   rlen;
	u_char					 	  *request;

	ngx_uint_t				  	   dfst_index;
	ngx_array_t				 	  *dfst_addrs;	/* ngx_addr_t */
	ngx_array_t				      *dfss_addrs;	/* ngx_addr_t */

//	ngx_http_dfst_handler_pt  handler;
	//ngx_pool_t 			  	  *pool;
};


//static void ngx_http_dfst_write_handler(ngx_event_t *wev);
static ngx_int_t ngx_http_dfst_create_request(ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx);
static ngx_int_t ngx_http_dfst_send_request(ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx);
static void ngx_http_dfst_read_handler(ngx_event_t *rev);
static void ngx_http_dfst_process_response(ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx, u_char *buf, size_t n);


static void *ngx_http_dfst_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_dfst_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);

static char *ngx_http_dfst_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


static uint32_t  ngx_http_dfst_request_counter = 0;

static ngx_command_t  ngx_http_dfst_commands[] = {
	{ ngx_string("dfst_pass"),
	  NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
	  ngx_http_dfst_pass,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  0,
	  NULL },
	
	{ ngx_string("dfst_timeout"),
	  NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	  ngx_conf_set_msec_slot,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  offsetof(ngx_http_dfst_loc_conf_t, timeout),
	  NULL }, 

	  ngx_null_command
};

static ngx_http_module_t  ngx_http_dfst_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_dfst_create_loc_conf,    	   /* create location configuration */
    ngx_http_dfst_merge_loc_conf      	   /* merge location configuration */
};


ngx_module_t  ngx_http_dfst_module = {
    NGX_MODULE_V1,
    &ngx_http_dfst_module_ctx,        	   /* module context */
    ngx_http_dfst_commands,           	   /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


void
ngx_http_dfst_init(ngx_http_request_t *r)
{
	ngx_uint_t				    i, j, nelts;
	ngx_addr_t				   *addr;
	ngx_http_upstream_server_t *server;
	ngx_http_dfst_ctx_t  	   *ctx;
	ngx_http_dfst_loc_conf_t   *dlcf;

	ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_dfst_ctx_t));
	if (ctx == NULL) {
		// ngx_mail_session_internal_server_error(s);
		return;
	}

	if (ngx_http_dfst_create_request(r, ctx) != NGX_OK) {
		// 
		return;
	}

	ngx_http_set_ctx(r, ctx, ngx_http_dfst_module);

	dlcf = ngx_http_get_module_loc_conf(r, ngx_http_dfst_module);

	nelts = dlcf->upstream.upstream->servers->nelts;
	server = dlcf->upstream.upstream->servers->elts;

	ctx->dfss_addrs = ngx_array_create(r->pool, 3, sizeof(ngx_addr_t));
	if (ctx->dfss_addrs == NULL) {
		//
		return;
	}

	ctx->dfst_addrs = ngx_array_create(r->pool, nelts, sizeof(ngx_addr_t));
	if (ctx->dfst_addrs == NULL) {
		//
		return;
	}
	
	j = ngx_http_dfst_request_counter % nelts;
	++ngx_http_dfst_request_counter;

	for ( i = j; i < nelts; ++i) {
		addr = ngx_array_push(ctx->dfst_addrs);	
		if (addr == NULL) {
			//
			return;
		}

		addr->sockaddr = server[i].addrs[0].sockaddr;
		addr->socklen = server[i].addrs[0].socklen;
		addr->name = server[i].addrs[0].name;
	}

	for (i = 0; i < j; ++i) {
		addr = ngx_array_push(ctx->dfst_addrs);	
		if (addr == NULL) {
			//
			return;
		}

		addr->sockaddr = server[i].addrs[0].sockaddr;
		addr->socklen = server[i].addrs[0].socklen;
		addr->name = server[i].addrs[0].name;
	}

	ctx->uc.log = *(r->connection->log);
	//ctx->uc.connection->data = r;
	//ctx->uc.connection->read->handler = ngx_http_dfst_read_handler;	

	ngx_http_dfst_send_request(r, ctx);
	//ctx->udp_connection->write->handler = ngx_http_dfst_write_handler;
	//ctx->peer.connection-> send
	//ctx->handler = ngx_http_dfst_process_response;
	
	// send quey
	//ctx->event->handler = ngx_http_dfst_timeout_handler;
	/*{
		size_t				 len;
		char 				 host[64], *p;
		struct sockaddr_in  *sin;
		
		sin = ngx_pcalloc(r->pool, sizeof(struct sockaddr_in));
		if (sin == NULL) {
			// 
			return;
		}

		sin->sin_family = AF_INET;
		sin->sin_addr.s_addr = inet_addr("10.101.69.16");
		sin->sin_port = htons(1304);

		len = (size_t) snprintf(host, sizeof(host), "%s:%hu", "10.101.69.16", 1304);
	
		p = ngx_pcalloc(r->pool, len);
		if (p == NULL) {
			//
			return;
		}
		
		memcpy(p, host, len);

		addr = ngx_array_push(ctx->dfss_addrs);
		if (addr == NULL) {
			//
			return;
		}

		addr->sockaddr = (struct sockaddr *) sin;
		addr->socklen = sizeof(struct sockaddr_in);
		addr->name.len = len;
		addr->name.data = (u_char *) p;
	}

	ngx_http_dfss_init(r, ctx->dfss_addrs);*/
}

static ngx_int_t
ngx_http_dfst_create_request(ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx)
{
	u_char  *b;

	ctx->request_header.body_n = ngx_http_dfst_request_counter;
	ctx->request_header.flen = 0;
	if (r->method & NGX_HTTP_GET) {
		ctx->request_header.rtype = NGX_HTTP_DFS_DOWNLOAD;
	} else if (r->method & NGX_HTTP_PUT) {
		ctx->request_header.rtype = NGX_HTTP_DFS_UPLOAD;
	} else {
		ctx->request_header.rtype = NGX_HTTP_DFS_DELETE;
	}
	
	if (ctx->request_header.rtype == NGX_HTTP_DFS_UPLOAD) {
		ctx->rlen = sizeof(ngx_http_dfs_request_header_t) + 2;
		ctx->request = ngx_palloc(r->pool, ctx->rlen);
		if (ctx->request == NULL) {
			return NGX_ERROR;
		}

		b = ctx->request;
		*((uint32_t *) b) = htonl(ctx->request_header.body_n);
		b += 8;
		*((uint16_t *) b) = htons(ctx->request_header.flen);
		b += 2;
		*((uint16_t *) b) = htons(ctx->request_header.rtype);
		b += 2;
		*((uint16_t *) b) = htons(1);
	} else {
		ctx->rlen = sizeof(ngx_http_dfs_request_header_t);
		ctx->request = ngx_palloc(r->pool, ctx->rlen);
		if (ctx->request == NULL) {
			return NGX_ERROR;
		}

		b = ctx->request;
		*((uint32_t *) b) = htonl(ctx->request_header.body_n);
		b += 8;
		*((uint16_t *) b) = htons(ctx->request_header.flen);
		b += 2;
		*((uint16_t *) b) = htons(ctx->request_header.rtype);
	}

	return NGX_OK;
}

static ngx_int_t
ngx_http_dfst_send_request(ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx)
{
	ssize_t			 	   n;
	ngx_uint_t			   i;
	ngx_addr_t 			  *addrs;
	ngx_udp_connection_t  *uc;

	//addr = (ngx_addr_t *) (ctx->addrs->elts + ctx->index + ctx->addrs->size);
	i = ctx->dfst_index++;
	addrs = ctx->dfst_addrs->elts;

	uc = &ctx->uc;
//	uc->log = *(r->connection->log);
	uc->sockaddr = addrs[i].sockaddr;
	uc->socklen = addrs[i].socklen;
	uc->server = addrs[i].name;

	if (ngx_udp_connect(uc) != NGX_OK) {
		return NGX_ERROR;
	}

	uc->connection->data = r;
	uc->connection->read->handler = ngx_http_dfst_read_handler;

	n = ngx_send(uc->connection, ctx->request, ctx->rlen);
	if (n == -1) {
		return NGX_ERROR;
	}

	if ((size_t) n != (size_t) ctx->rlen) {
		ngx_log_error(NGX_LOG_CRIT, &uc->log, 0, "send() incomplete");
		return NGX_ERROR;
	}

	// add timer
	
	return NGX_OK;
}

static void
ngx_http_dfst_read_handler(ngx_event_t *rev)
{
	ssize_t  			  n;
	u_char 			   	  buf[512];
	ngx_connection_t     *c;
	ngx_http_request_t   *r;
	ngx_http_dfst_ctx_t  *ctx;


	c = rev->data;
	r = c->data;
	ctx = ngx_http_get_module_ctx(r, ngx_http_dfst_module);

	do {
		n = ngx_udp_recv(c, buf, 512);

		if (n < 0) {	
			return;
		}

		ngx_http_dfst_process_response(r, ctx, buf, n);
	} while (rev->ready);

	//panduan

//	ngx_http_dfss_init(r, ctx->dfss_addrs);
}

static void 
ngx_http_dfst_process_response(ngx_http_request_t *r, ngx_http_dfst_ctx_t *ctx, u_char *buf, size_t n)
{
	char						   *err;
	u_char						   *b, *p;
	uint16_t						idc_id;
	uint32_t					    i, j, id;
	size_t							len;
	//in_port_t						port;
	//in_add_t					 	in_addr;
	struct sockaddr_in			   *sin;
	ngx_addr_t					   *addr;
	//ngx_http_dfst_ctx_t			   *ctx;
	ngx_http_dfs_response_header_t  header;

	//ctx = ngx_http_get_module_ctx(r, ngx_http_dfst_module);
	
	id = ctx->request_header.body_n;
	if (n < sizeof(ngx_http_dfs_response_header_t)) {
		goto short_response;
	}

	b = buf;
	header.body_n = ntohl(*((uint32_t *) b));
	b += 10;
	header.rcode = ntohs(*((uint16_t *) b));
	b += 2;

	ngx_log_debug3(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http dfst response %ui body_n:%ui rcode:%ui", 
				n, header.body_n, header.rcode);

	if (header.rcode != NGX_HTTP_DFS_OK) {
		goto dfst_error;
	}

	if (header.body_n != id) {
		goto dfst_error;
	}
 
	j = (n - sizeof(ngx_http_dfs_response_header_t)) / 8;
	for (i = 0; i < j; ++i) {
		addr = ngx_array_push(ctx->dfss_addrs);
		if (addr == NULL) {
			return;
		}

		sin = ngx_pcalloc(r->pool, sizeof(struct sockaddr_in));
		if (sin == NULL) {
			return;
		}

		idc_id = ntohs(*((uint16_t *) b));
		b += 2;
		sin->sin_family = AF_INET;
		sin->sin_port = *((uint16_t *) b);
		b += 2;
		sin->sin_addr.s_addr = *((uint32_t *) b);
		b += 4;

		addr->sockaddr = (struct sockaddr *) sin;
		addr->socklen = sizeof(struct sockaddr_in);
		
		len = NGX_INET_ADDRSTRLEN + sizeof(":65535") - 1;
		
		p = ngx_pnalloc(r->pool, len);
		if (p == NULL) {
			return;
		}

		len = ngx_sock_ntop((struct sockaddr *) sin, sizeof(struct sockaddr_in), p, len, 1);

		addr->name.len = len;
		addr->name.data = p;

		ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "dfss %ui ip:port %V", idc_id, &addr->name);
	}

	ngx_http_dfss_init(r, ctx->dfss_addrs);

	return;

short_response:
	
	err = "short dfst response";

	return;

//done:

//	ngx_log_error(NGX_LOG_ERROR, r->connection->log, 0, err);

//	return;

dfst_error:
	
	ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "dfst error(%ui: %s), request id:%ui => %ui", 
				header.rcode, ngx_http_dfs_strerror(header.rcode), id, header.body_n);

	return;
}

/*static void
ngx_http_dfst_write_handler(ngx_event_t *wev)
{
	
	b = ctx->request;
	size = b->last - b->pos;
	n = ngx_send(uc->connection, b->pos, size);
	if (n == -1) {
	}

	if ((size_t) n != size) {
		ngx_log_error(NGX_LOG_CRIT, uc->log, 0, "send() incomplete");
		// 
	}

	ngx_add_timer(ctx->peer.connection->read, 6000);
}*/


static void *
ngx_http_dfst_create_loc_conf(ngx_conf_t *cf)
{
	ngx_http_dfst_loc_conf_t  *conf;

	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_dfst_loc_conf_t));
	if (conf == NULL) {
		return NULL;
	}

	conf->upstream.read_timeout = NGX_CONF_UNSET_MSEC;

	return conf;
}

static char *
ngx_http_dfst_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	ngx_http_dfst_loc_conf_t *prev = parent;
	ngx_http_dfst_loc_conf_t *conf = child;

	ngx_conf_merge_msec_value(conf->upstream.read_timeout,
							  prev->upstream.read_timeout, 6000);


	return NGX_CONF_OK;
}

static char *
ngx_http_dfst_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_dfst_loc_conf_t *dlcf = conf;

	ngx_str_t  				  *value;
	ngx_url_t 				   u;
	ngx_http_core_loc_conf_t  *clcf;

	if (dlcf->upstream.upstream) {
		return "is duplicate";
	}

	value = cf->args->elts;

	ngx_memzero(&u, sizeof(ngx_url_t));
	u.url = value[1];
	u.no_resolve = 1;

	dlcf->upstream.upstream = ngx_http_upstream_add(cf, &u, 0);
	if (dlcf->upstream.upstream == NULL) {
		return NGX_CONF_ERROR;
	}

	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

	// clcf->handler = ngx_http_dfst_handler;

	if (clcf->name.data[clcf->name.len - 1] == '/') {
		clcf->auto_redirect = 1;
	}

	return NGX_CONF_OK;
}
