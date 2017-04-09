
/*
 * Copyright (C) xianliang.li
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_http_dfs.h>


#define NGX_HTTP_DFS_FID_HEADER	"X-dfs_fid"


typedef struct {
	ngx_http_upstream_conf_t  upstream;
} ngx_http_dfss_loc_conf_t;

typedef struct {
	size_t					  rest;
	ngx_uint_t				  index;
	ngx_array_t				 *addrs;
} ngx_http_dfss_ctx_t;


static ngx_int_t ngx_http_dfss_create_request_put(ngx_http_request_t *r);
static ngx_int_t ngx_http_dfss_create_request_get(ngx_http_request_t *r);
static ngx_int_t ngx_http_dfss_create_request_delete(ngx_http_request_t *r);
static ngx_int_t ngx_http_dfss_reinit_request(ngx_http_request_t *r);
static ngx_int_t ngx_http_dfss_process_header_put(ngx_http_request_t *r);
static ngx_int_t ngx_http_dfss_process_header(ngx_http_request_t *r);
/*static ngx_int_t ngx_http_dfss_input_filter_init(void *data);
static ngx_int_t ngx_http_dfss_filter(void *data, ssize_t bytes);*/
static void	     ngx_http_dfss_abort_request(ngx_http_request_t *r);
static void		 ngx_http_dfss_finalize_request(ngx_http_request_t *r, ngx_int_t rc);

//static char *ngx_http_dfss_strerror(ngx_int_t err);

static void *ngx_http_dfss_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_dfss_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);

static char *ngx_http_dfss_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


/*static ngx_conf_bitmask_t ngx_http_dfss_next_upstream_masks[] = {
	{ ngx_string("error"), NGX_HTTP_UPSTREAM_FT_ERROR },
	{ ngx_string("timeout"), NGX_HTTP_UPSTREAM_FT_TIMEOUT },
	{ ngx_string("invalid_response"), NGX_HTTP_UPSTREAM_FT_INVALID_HEADER },
	{ ngx_string("off"), NGX_HTTP_UPSTREAM_FT_OFF },
	{ ngx_null_string, 0 }
};*/

static ngx_command_t  ngx_http_dfss_commands[] = {
	{ ngx_string("dfss_pass"),
	  NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
	  ngx_http_dfss_pass,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  0,
	  NULL },

	{ ngx_string("dfss_connect_timeout"),
	   NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	   ngx_conf_set_msec_slot,
	   NGX_HTTP_LOC_CONF_OFFSET,
	   offsetof(ngx_http_dfss_loc_conf_t, upstream.connect_timeout),
	   NULL },

	{ ngx_string("dfss_send_timeout"),
	  NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	  ngx_conf_set_msec_slot,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  offsetof(ngx_http_dfss_loc_conf_t, upstream.send_timeout),
	  NULL },

	{ ngx_string("dfss_read_timeout"),
	  NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	  ngx_conf_set_msec_slot,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  offsetof(ngx_http_dfss_loc_conf_t, upstream.read_timeout),
	  NULL },

	{ ngx_string("dfss_send_lowat"),
	  NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	  ngx_conf_set_msec_slot,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  offsetof(ngx_http_dfss_loc_conf_t, upstream.send_lowat),
      NULL },

	ngx_null_command
};


static ngx_http_module_t  ngx_http_dfss_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_dfss_create_loc_conf,    	   /* create location configuration */
    ngx_http_dfss_merge_loc_conf      	   /* merge location configuration */
};


ngx_module_t  ngx_http_dfss_module = {
    NGX_MODULE_V1,
    &ngx_http_dfss_module_ctx,        	   /* module context */
    ngx_http_dfss_commands,           	   /* module directives */
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


static ngx_int_t
ngx_http_dfss_handler(ngx_http_request_t *r)
{
	ngx_int_t				   rc, read_body = 0;
	ngx_http_upstream_t  	  *u;
	ngx_http_dfss_ctx_t       *ctx;
	ngx_http_dfss_loc_conf_t  *dlcf;

	/*if (!(r->method & (NGX_HTTP_GET|NGX_HTTP_PUT|NGX_HTTP_DELETE))) {
		return NGX_HTTP_NOT_ALLOWED;
	}*/

	if (ngx_http_upstream_create(r) != NGX_OK) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	u = r->upstream;

/*	{
		u->resolved = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_resolved_t));
		if (u->resolved == NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}

		size_t				 len;
		char 				 host[64], *p;
		struct sockaddr_in  *sin;
		
		sin = ngx_pcalloc(r->pool, sizeof(struct sockaddr_in));
		if (sin == NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}

		sin->sin_family = AF_INET;
		sin->sin_addr.s_addr = inet_addr("10.101.69.16");
		sin->sin_port = htons(1304);

		len = (size_t) snprintf(host, sizeof(host), "%s:%hu", "10.101.69.16", 1304);
	
		p = ngx_pcalloc(r->pool, len);
		if (p == NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		
		memcpy(p, host, len);

		u->resolved->sockaddr = (struct sockaddr *) sin;
		u->resolved->socklen = sizeof(struct sockaddr_in);
		u->resolved->naddrs = 1;
		u->resolved->host.len = len;
		u->resolved->host.data = (u_char *) p;
	} */

	ngx_str_set(&u->schema, "dfss://");
	u->output.tag = (ngx_buf_tag_t) &ngx_http_dfss_module;
	dlcf = ngx_http_get_module_loc_conf(r, ngx_http_dfss_module);
	
	u->conf = &dlcf->upstream;
	
	u->reinit_request = ngx_http_dfss_reinit_request;
	//u->process_header = ngx_http_dfss_process_header;
	u->abort_request = ngx_http_dfss_abort_request;
	u->finalize_request = ngx_http_dfss_finalize_request;
	
	ctx = ngx_palloc(r->pool, sizeof(ngx_http_dfss_ctx_t));
	if (ctx == NULL) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	ngx_http_set_ctx(r, ctx, ngx_http_dfss_module);
	
	//u->input_filter_init = ngx_http_dfss_input_filter_init;
	//u->input_filter = ngx_http_dfss_filter;
	//u->input_filter_ctx = ctx;

	if (r->method == NGX_HTTP_GET) {
		u->create_request = ngx_http_dfss_create_request_get;
		u->process_header = ngx_http_dfss_process_header;
	} else if (r->method == NGX_HTTP_PUT) {
		read_body = 1;
		u->create_request = ngx_http_dfss_create_request_put;
		u->process_header = ngx_http_dfss_process_header_put;
	} else if (r->method == NGX_HTTP_DELETE){
		u->create_request = ngx_http_dfss_create_request_delete;
		u->process_header = ngx_http_dfss_process_header;
	} else {
		return NGX_HTTP_NOT_ALLOWED;
	}

	if (read_body) {
		rc = ngx_http_read_client_request_body(r, ngx_http_dfst_init);

		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			return rc;
		}
	} else {
		rc = ngx_http_discard_request_body(r);

		if (rc != NGX_OK) {
			return rc;
		}

		r->main->count++;

		ngx_http_dfst_init(r);
	}

	return NGX_DONE;
}

void
ngx_http_dfss_init(ngx_http_request_t *r, ngx_array_t *addrs)
{
	ngx_uint_t  		  nelts;
	ngx_addr_t			 *addr;
	ngx_http_dfss_ctx_t  *ctx;
	ngx_http_upstream_t  *u;

	ctx = ngx_http_get_module_ctx(r, ngx_http_dfss_module);
	
	ctx->index = 0;
	ctx->addrs = addrs;
	addr = addrs->elts;

	nelts = addrs->nelts;
	if (nelts == 0) {
		// 
		return;
	}

	u = r->upstream;

	 
	u->resolved = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_resolved_t));
	if (u->resolved == NULL) {
		//return NGX_HTTP_INTERNAL_SERVER_ERROR;
		//
		return;
	}

	u->resolved->sockaddr = addr[ctx->index].sockaddr;
	u->resolved->socklen = addr[ctx->index].socklen;
	u->resolved->naddrs = 1;
	u->resolved->host = addr[ctx->index].name;

	ngx_http_upstream_init(r);
}

static ngx_int_t
ngx_http_dfss_create_request_put(ngx_http_request_t *r)
{
	size_t			 bytes;
	uint16_t		 flen, qtype;
	uint32_t		 body_n;
	ngx_buf_t		*b;
	ngx_chain_t	    *cl, *in;

	b = ngx_create_temp_buf(r->pool, 8);
	if (b == NULL) {
		return NGX_ERROR;
	}

	cl = ngx_alloc_chain_link(r->pool);
	if (cl == NULL) {
		return NGX_ERROR;
	}

	cl->buf = b;
	cl->next = NULL;

	r->upstream->request_bufs = cl;

	bytes = 0;
	for (in = r->request_body->bufs; in; in = in->next) {
		bytes += ngx_buf_size(in->buf);
	}

	if (bytes != (uint32_t) r->headers_in.content_length_n) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "http dfss put: wrong content length size, header %d, found %d",
				r->headers_in.content_length_n, bytes);

		return NGX_ERROR;
	}

	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http dfss put: size %d", bytes);

	body_n = htonl(r->headers_in.content_length_n);
	flen = 0;
	qtype = htons(NGX_HTTP_DFS_UPLOAD);
	ngx_memcpy(b->last, &body_n, 4);
	b->last += 4;
	ngx_memcpy(b->last, &flen, 2);
	b->last += 2;
	ngx_memcpy(b->last, &qtype, 2);
	b->last += 2;

	in = r->request_body->bufs;
	
	while (in) {
		cl->next = ngx_alloc_chain_link(r->pool);
		if (cl->next == NULL) {
			return NGX_ERROR;
		}

		cl = cl->next;
		cl->buf = ngx_calloc_buf(r->pool);
		if (cl->buf == NULL) {
			return NGX_ERROR;
		}

		cl->buf->memory = 1;
		*cl->buf = *in->buf;
		in = in->next;
	}
	
	return NGX_OK;
}

static ngx_int_t
ngx_http_dfss_create_request_get(ngx_http_request_t *r)
{
	size_t	      prefix;
	uint16_t      flen, qtype;
	uint32_t      body_n;
	ngx_str_t     fid;
	ngx_buf_t    *b;
	ngx_chain_t  *cl;

	for (prefix = r->uri.len; prefix; --prefix) {
		if (r->uri.data[prefix - 1] == '/') {
			break;
		}
	}

	fid.len = r->uri.len - prefix;
	fid.data = r->uri.data + prefix;

	if (fid.len == 0) {
		// badrequest
	}

	b = ngx_create_temp_buf(r->pool, fid.len + 8);
	if (b == NULL) {
		return NGX_ERROR;
	}

	cl = ngx_alloc_chain_link(r->pool);
	if (cl == NULL) {
		return NGX_ERROR;
	}

	cl->buf = b;
	cl->next = NULL;

	r->upstream->request_bufs = cl;

	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http dfss get: \"%V\"", &fid);

	body_n = htonl(fid.len);
	flen = 0;
	qtype = htons(NGX_HTTP_DFS_DOWNLOAD);
	memcpy(b->last, &body_n, 4);
	b->last += 4;
	memcpy(b->last, &flen, 2);
	b->last += 2;
	memcpy(b->last, &qtype, 2);
	b->last += 2;
	memcpy(b->last, fid.data, fid.len);
	b->last += fid.len;

	return NGX_OK;
}

static ngx_int_t
ngx_http_dfss_create_request_delete(ngx_http_request_t *r)
{
	size_t	      prefix;
	uint16_t      flen, qtype;
	uint32_t      body_n;
	ngx_str_t     fid;
	ngx_buf_t    *b;
	ngx_chain_t  *cl;

	for (prefix = r->uri.len; prefix; --prefix) {
		if (r->uri.data[prefix - 1] == '/') {
			break;
		}
	}

	fid.len = r->uri.len - prefix;
	fid.data = r->uri.data + prefix;

	if (fid.len == 0) {
		// badrequest
	}

	b = ngx_create_temp_buf(r->pool, fid.len + 8);
	if (b == NULL) {
		return NGX_ERROR;
	}

	cl = ngx_alloc_chain_link(r->pool);
	if (cl == NULL) {
		return NGX_ERROR;
	}

	cl->buf = b;
	cl->next = NULL;

	r->upstream->request_bufs = cl;

	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http dfss delete: \"%V\"", &fid);

	body_n = htonl(fid.len);
	flen = 0;
	qtype = htons(NGX_HTTP_DFS_DELETE);
	memcpy(b->last, &body_n, 4);
	b->last += 4;
	memcpy(b->last, &flen, 2);
	b->last += 2;
	memcpy(b->last, &qtype, 2);
	b->last += 2;
	memcpy(b->last, fid.data, fid.len);
	b->last += fid.len;

	return NGX_OK;
}

static ngx_int_t
ngx_http_dfss_reinit_request(ngx_http_request_t *r)
{
	return NGX_OK;
}

static ngx_int_t
ngx_http_dfss_process_header_put(ngx_http_request_t *r)
{
	size_t 				  len;
	uint16_t			  rcode;
	uint32_t			  body_n;
	ngx_str_t			  fid;
	ngx_buf_t			 *b;
	ngx_table_elt_t		 *e;
	ngx_http_upstream_t  *u;

	u = r->upstream;

	b = &u->buffer;
	len = b->last - b->pos;
	if (len > 8) {
		body_n = ntohl(*((uint32_t *) b->pos));
		b->pos += 6;
		rcode = ntohs(*((uint16_t *) b->pos));
		b->pos += 2;

		if (len >= body_n + 8) {
			fid.data = ngx_pnalloc(r->pool, body_n);
			if (fid.data == NULL) {
				return NGX_ERROR;
			}

			fid.len = body_n;	
			memcpy(fid.data, b->pos, body_n);

			ngx_log_debug3(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http dfss parser: %uD %uD \"%V\"", body_n, rcode, &fid);
		
			e = ngx_list_push(&r->headers_out.headers);
			if (e == NULL) {
				return NGX_ERROR;
			}

			e->hash = 1;
			ngx_str_set(&e->key, NGX_HTTP_DFS_FID_HEADER);
			e->value = fid;

			//ctx->rest = body_n;
			/*u->headers_in.content_length_n = body_n;
			u->headers_in.status_n = 200;
			u->length = body_n;*/
		
			u->headers_in.content_length_n = 0;
			u->headers_in.status_n = 200;

			return NGX_OK;
		}

		return NGX_EAGAIN;
	}

	return NGX_EAGAIN;
}

static ngx_int_t
ngx_http_dfss_process_header(ngx_http_request_t *r)
{
	size_t 				  len;
	uint16_t			  rcode;
	uint32_t			  body_n;
	ngx_buf_t			 *b;
	ngx_http_upstream_t  *u;

	u = r->upstream;

	b = &u->buffer;
	len = b->last - b->pos;
	if (len > 8) {
		body_n = ntohl(*((uint32_t *) b->pos));
		b->pos += 6;
		rcode = ntohs(*((uint16_t *) b->pos));
		b->pos += 2;

		ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http dfss parser: %uD %uD", body_n, rcode);

		//ctx->rest = body_n;
		u->headers_in.content_length_n = body_n;
		u->headers_in.status_n = 200;
		u->length = body_n;

		return NGX_OK;
	}

	return NGX_EAGAIN;
}

/*static ngx_int_t
ngx_http_dfss_input_filter_init(void *data)
{
	return NGX_OK;
}

static ngx_int_t 
ngx_http_dfss_filter(void *data, ssize_t bytes)
{
	return NGX_OK;
}
*/
static void 
ngx_http_dfss_abort_request(ngx_http_request_t *r)
{
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "abort http dfss request");

	return;
}

static void
ngx_http_dfss_finalize_request(ngx_http_request_t *r, ngx_int_t rc)
{
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "finalize http dfss request");

	return;
}

/*static char *
ngx_http_dfss_strerror(ngx_int_t err)
{
	static char *errors[] = {
		"",
		""
	};

	if (err >= 400 && err < 410) {
		return errors[err - 1];
	}

	return "Unknown error";
}*/

static void *
ngx_http_dfss_create_loc_conf(ngx_conf_t *cf)
{
	ngx_http_dfss_loc_conf_t  *conf;

	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_dfss_loc_conf_t));
	if (conf == NULL) {
		return NULL;
	}

	conf->upstream.connect_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.send_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.read_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.send_lowat = 0;
	conf->upstream.buffer_size = NGX_CONF_UNSET_SIZE;

	return conf;
}

static char *
ngx_http_dfss_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	ngx_http_dfss_loc_conf_t *prev = parent;
	ngx_http_dfss_loc_conf_t *conf = child;

	ngx_conf_merge_msec_value(conf->upstream.connect_timeout,
							  prev->upstream.connect_timeout, 60000);

	ngx_conf_merge_msec_value(conf->upstream.send_timeout,
							  prev->upstream.send_timeout, 60000);

	ngx_conf_merge_msec_value(conf->upstream.read_timeout,
							  prev->upstream.read_timeout, 60000);

	ngx_conf_merge_size_value(conf->upstream.send_lowat,
							  prev->upstream.send_lowat, 0);
	
	ngx_conf_merge_size_value(conf->upstream.buffer_size, 
							  prev->upstream.buffer_size, 1024)

	return NGX_CONF_OK;
}

static char *
ngx_http_dfss_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_dfss_loc_conf_t *dlcf = conf;

	ngx_str_t				  *value;
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

	clcf->handler = ngx_http_dfss_handler;

	if (clcf->name.data[clcf->name.len - 1] == '/') {
		clcf->auto_redirect = 1;
	}

	return NGX_CONF_OK;
}
