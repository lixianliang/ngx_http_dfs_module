
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 * Copytight (C) xianliang.li
 */


#ifndef _NGX_HTTP_DFS_INCLUDE_
#define _NGX_HTTP_DFS_INCLUDE_



#include <ngx_config.h>
#include <ngx_core.h>


#define NGX_HTTP_DFS_DOWNLOAD				0
#define NGX_HTTP_DFS_UPLOAD					1
#define NGX_HTTP_DFS_UPLOAD_SC 				2
#define NGX_HTTP_DFS_DELETE					3

#define NGX_HTTP_DFS_OK						200
#define NGX_HTTP_DFS_BAD_REQUEST            400
#define NGX_HTTP_DFS_NOT_FIND_IDC           401
#define NGX_HTTP_DFS_NOT_FIND_STORAGE       402
#define NGX_HTTP_DFS_NOT_FIND_FID           403
#define NGX_HTTP_DFS_REQUEST_TIMEOUT        404
#define NGX_HTTP_DFS_CLIENT_CLOSE_REQUEST   405
#define NGX_HTTP_DFS_SERVER_ERROR           406
#define NGX_HTTP_DFS_NOT_IMPLEMENTED        407
#define NGX_HTTP_DFS_BAD_GATEWAY            408


#pragma pack(2)
typedef struct {
	uint64_t	body_n;
	uint16_t	flen;
	uint16_t	rtype;
} ngx_http_dfs_request_header_t;
#pragma pack()

#pragma pack(2)
typedef struct {
	uint64_t    body_n;
	uint16_t	flen;
	uint16_t	rcode;
} ngx_http_dfs_response_header_t;
#pragma pack()


extern ngx_int_t ngx_udp_connect(ngx_udp_connection_t *uc);

void ngx_http_dfss_init(ngx_http_request_t *r, ngx_array_t *addrs);
void ngx_http_dfst_init(ngx_http_request_t *r);


static inline char *
ngx_dfs_strerror(ngx_int_t err)
{
    static char *errors[] = {
        "Bad request",
        "Idc not find",
        "Storage not find",
        "Fid not find",
        "Request timeout",
        "Client close request",
        "Server failure",
        "Not implemented",
        "Bad gateway"
    };

    if (err >= 400 && err < 409) {
        return errors[err - 400];
    }

    return "Unknown error";
}


#endif	/* _NGX_HTTP_DFS_INCLUDE_ */
