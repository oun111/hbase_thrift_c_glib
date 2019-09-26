#include "hclient.h"


int test_t_h_base_service_client()
{
  struct hbase_client_s client ;
  gchar *host = "127.0.0.1";
  gint port = 9090;
  gchar tmp[256] = "";
  

  if (hclient_init(&client,host,port)) {
    goto __done ;
  }


  /*
   * case 1: 
   */
  hclient_exists(&client,"user2");

  /*
   * case 2: 
   */
  hclient_get(&client,"user1","no-001001","name",tmp);
  printf("name: %s\n",tmp);
  hclient_get(&client,"user1","no-001001","age",tmp);
  printf("age: %s\n",tmp);

  /*
   * case 3: 
   */
  hclient_put(&client,"user1","no-001001","name","JOey");
  hclient_put(&client,"user1","no-001001","age","33");
  hclient_put(&client,"user1","no-001002","name","顺风");
  hclient_put(&client,"user1","no-001002","age","37");
  hclient_put(&client,"user1","no-001002","sex","男");

  /*
   * 
   */
  hclient_get(&client,"user1","no-001002","sex",tmp);
  printf("sex: %s\n",tmp);

  /*
   * case 4:
   */
  hclient_delete(&client,"user1","no-001002","");

__done:
  hclient_release(&client);

  return 0;
}

int main()
{
  test_t_h_base_service_client();
  return 0;
}

