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


int test_exists(THBaseServiceIf *client, const gchar *tablename)
{
  GByteArray *table = NULL ;
  TGet *tget = NULL ;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;


  printf("testing 'exists' command for talbe '%s'...\n",tablename);

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

int test_get(THBaseServiceIf *client, const gchar *tablename)
{
  GByteArray *table = NULL ;
  TGet *tget = NULL ;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;
  TResult *result= NULL;
  gchar *rno = "no-001001";


  printf("testing 'get' command for talbe '%s'...\n",tablename);

  // construct TGet
  tget = g_object_new(TYPE_T_GET,NULL);
  tget->row = g_byte_array_new();
  g_byte_array_append(tget->row,(guint8*)rno,strlen(rno));

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
    printf("row: %s\n",result->row->data);

    for (int i=0;i<result->columnValues->len;i++) {
      TColumnValue *pcol = result->columnValues->pdata[i];
      printf("col family '%s', value '%s'\n",(char*)pcol->family->data,(char*)pcol->value->data);
    }

    printf("success!\n");
  }

  g_byte_array_unref(table);
  g_object_unref(tget);
  g_object_unref(result);
  g_object_unref(io);

  return 0;
}

int test_put(THBaseServiceIf *client, const gchar *tablename, 
             const gchar *cf, const gchar *val)
{
  GByteArray *table = NULL ;
  TPut *tput = NULL ;
  gboolean ret = FALSE;
  TIOError *io = NULL;
  GError *error = NULL;
  gchar *rno = "no-001001";


  printf("testing 'put' command for talbe '%s'...\n",tablename);

  // construct TPut
  tput = g_object_new(TYPE_T_PUT,NULL);
  tput->row = g_byte_array_new();
  g_byte_array_append(tput->row,(guint8*)rno,strlen(rno));
#if 0
  tput->columnValues = g_ptr_array_new();
  g_ptr_array_add(tput->columnValues,(gpointer)cf);
  g_ptr_array_add(tput->columnValues,(gpointer)val);
#endif

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
  test_exists(client,"user2");

  /*
   * case 2: 
   */
  //test_get(client,"user1");

  /*
   * case 3: 
   */
  test_put(client,"user1","name","JOey");

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

