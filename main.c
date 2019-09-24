#include "t_h_base_service.h"
#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/thrift_application_exception.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>
#include <thrift/c_glib/protocol/thrift_compact_protocol.h>
#include <thrift/c_glib/protocol/thrift_multiplexed_protocol.h>
#include <thrift/c_glib/transport/thrift_buffered_transport.h>
#include <thrift/c_glib/transport/thrift_framed_transport.h>
#include <thrift/c_glib/transport/thrift_ssl_socket.h>
#include <thrift/c_glib/transport/thrift_socket.h>
#include <thrift/c_glib/transport/thrift_transport.h>


int hclient_exists(THBaseServiceIf *client, const gchar *tablename)
{
  GByteArray *table = NULL ;
  TGet *tget = NULL ;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;


  printf("testing 'exists' command for talbe '%s'*******\n",tablename);

  tget = g_object_new(TYPE_T_GET,NULL);

  tget->row = g_byte_array_new();
  g_byte_array_append(tget->row,(guint8*)"0",strlen("0"));

  table= g_byte_array_new();
  g_byte_array_append(table,(guint8*)tablename,strlen(tablename));


  ret = t_h_base_service_client_exists(client,&ret,table,tget,&io,&error);  
  if (ret==FALSE) {
    printf("client exists fail(%d): %s (%s)\n",
           error->code,error->message,io->message);
  }
  else {
    printf("success!\n");
  }

  g_byte_array_unref(table);
  g_object_unref(tget);
  g_object_unref(io);

  return 0;
}

int hclient_get(THBaseServiceIf *client, const gchar *tablename, const gchar *row, 
                const gchar *cf, gchar *val)
{
  GByteArray *table = NULL ;
  TGet *tget = NULL ;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;
  TResult *result= NULL;


  printf("testing 'get' command for talbe '%s'*******\n",tablename);

  // construct TGet
  tget = g_object_new(TYPE_T_GET,NULL);
  tget->row = g_byte_array_new();
  g_byte_array_append(tget->row,(guint8*)row,strlen(row));

  // construct TColumn
  if (cf && strlen(cf)>0) {
    TColumn *column = g_object_new(TYPE_T_COLUMN,NULL);
    column->family = g_byte_array_new();
    g_byte_array_append(column->family,(guint8*)cf,strlen(cf));
    tget->__isset_columns = TRUE;
    g_ptr_array_add(tget->columns,column);
  }

  // construct table
  table= g_byte_array_new();
  g_byte_array_append(table,(guint8*)tablename,strlen(tablename));

  // result
  result = g_object_new(TYPE_T_RESULT,NULL);

  ret = t_h_base_service_client_get(client,&result,table,tget,&io,&error);  
  if (ret==FALSE) {
    printf("client get fail(%d): %s (%s)\n",error?error->code:-1,
           error?error->message:"n/A",io?io->message:"N/a");
  }
  else {
    printf("row: %s\n",result->__isset_row?(gchar*)result->row->data:"none");

    for (int i=0;i<result->columnValues->len;i++) {
      TColumnValue *pcol = result->columnValues->pdata[i];
      printf("col family '%s'[%d], value '%s'[%d], qualifier '%s'\n",
             (gchar*)pcol->family->data,pcol->family->len,
             (gchar*)pcol->value->data,pcol->value->len,
             (gchar*)pcol->qualifier->data);

      if (val) {
        strncpy(val,(gpointer)pcol->value->data,pcol->value->len);
        val[pcol->value->len] = '\0';
      }
    }

    printf("success!\n");
  }

  g_byte_array_unref(table);
  g_object_unref(tget);
  g_object_unref(result);
  if (G_IS_OBJECT(io))
    g_object_unref(io);

  return 0;
}

int hclient_put(THBaseServiceIf *client, const gchar *tablename, 
             const gchar *row, const gchar *cf, const gchar *val)
{
  GByteArray *table = NULL ;
  TPut *tput = NULL ;
  TColumnValue *cval = NULL;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;


  printf("testing 'put' command for talbe '%s'*******\n",tablename);

  // construct TPut
  tput = g_object_new(TYPE_T_PUT,NULL);
  tput->row = g_byte_array_new();
  g_byte_array_append(tput->row,(guint8*)row,strlen(row));

  // columns
  cval = g_object_new(TYPE_T_COLUMN_VALUE,NULL);
  // family
  cval->family = g_byte_array_new();
  g_byte_array_append(cval->family,(guint8*)cf,strlen(cf));
  // value
  cval->value = g_byte_array_new();
  g_byte_array_append(cval->value,(guint8*)val,strlen(val));
  // qualifier
  cval->qualifier = NULL;
  g_ptr_array_add(tput->columnValues,(gpointer)cval);

  // construct table
  table= g_byte_array_new();
  g_byte_array_append(table,(guint8*)tablename,strlen(tablename));

  ret = t_h_base_service_client_put(client,table,tput,&io,&error);  
  if (ret==FALSE) {
    printf("client put fail(%d): %s (%s)\n",error?error->code:-1,
           error?error->message:"n/A",io?io->message:"N/a");
  }
  else {
    printf("success!\n");
  }

  g_byte_array_unref(table);
  g_object_unref(tput);
  if (G_IS_OBJECT(io))
    g_object_unref(io);
  if (G_IS_OBJECT(cval))
    g_object_unref(cval);

  return 0;
}

int hclient_delete(THBaseServiceIf *client, const gchar *tablename, 
                const gchar *row, const gchar *cf)
{
  GByteArray *table = NULL ;
  TDelete *tdel = NULL ;
  TColumn *cval = NULL;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;


  printf("testing 'delete' command for talbe '%s'*******\n",tablename);

  // construct TDelete
  tdel = g_object_new(TYPE_T_DELETE,NULL);
  tdel->row = g_byte_array_new();
  g_byte_array_append(tdel->row,(guint8*)row,strlen(row));

  // columns
  if (cf && strlen(cf)>0) {
    cval = g_object_new(TYPE_T_COLUMN,NULL);
    // family
    cval->family = g_byte_array_new();
    g_byte_array_append(cval->family,(guint8*)cf,strlen(cf));
    tdel->__isset_columns = TRUE;
    g_ptr_array_add(tdel->columns,(gpointer)cval);
  }

  // construct table
  table= g_byte_array_new();
  g_byte_array_append(table,(guint8*)tablename,strlen(tablename));

  ret = t_h_base_service_client_delete_single(client,table,tdel,&io,&error);  
  if (ret==FALSE) {
    printf("client delete fail(%d): %s (%s)\n",error?error->code:-1,
           error?error->message:"n/A",io?io->message:"N/a");
  }
  else {
    printf("success!\n");
  }

  g_byte_array_unref(table);
  g_object_unref(tdel);
  if (G_IS_OBJECT(io))
    g_object_unref(io);

  return 0;
}

int test_t_h_base_service_client()
{
  ThriftSocket *socket = NULL;
  ThriftProtocol *protocol= NULL;
  THBaseServiceIf *client = NULL;
  ThriftTransport *transport = NULL;
  gchar *host = "127.0.0.1";
  gint port = 9090;
  gchar tmp[256] = "";
  

  socket = g_object_new(THRIFT_TYPE_SOCKET,"hostname",host,
                        "port",port,NULL);


  transport = g_object_new(THRIFT_TYPE_BUFFERED_TRANSPORT,
                           "transport",socket,NULL);

  thrift_transport_open(transport, NULL);
  if (!thrift_transport_is_open(transport)) {
    printf("open socket fail!\n");
    goto __done;
  }

  protocol = g_object_new(THRIFT_TYPE_BINARY_PROTOCOL,
                          "transport",transport,NULL);

  client = g_object_new(TYPE_T_H_BASE_SERVICE_CLIENT,
                        "input_protocol",protocol,
                        "output_protocol",protocol,
                        NULL);

  /*
   * case 1: 
   */
  hclient_exists(client,"user2");

  /*
   * case 2: 
   */
  hclient_get(client,"user1","no-001001","name",tmp);
  printf("name: %s\n",tmp);
  hclient_get(client,"user1","no-001001","age",tmp);
  printf("age: %s\n",tmp);

  /*
   * case 3: 
   */
  hclient_put(client,"user1","no-001001","name","JOey");
  hclient_put(client,"user1","no-001001","age","33");
  hclient_put(client,"user1","no-001002","name","顺风");
  hclient_put(client,"user1","no-001002","age","37");
  hclient_put(client,"user1","no-001002","sex","男");

  /*
   * 
   */
  hclient_get(client,"user1","no-001002","sex",tmp);
  printf("sex: %s\n",tmp);

  /*
   * case 4:
   */
  hclient_delete(client,"user1","no-001002","");

__done:
  g_object_unref(client);
  g_object_unref(protocol);
  g_object_unref(transport);
  g_object_unref(socket);

  return 0;
}

int main()
{
  test_t_h_base_service_client();
  return 0;
}

