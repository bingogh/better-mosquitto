/*
  Copyright (c) 2009-2013 Roger Light <roger@atchoo.org>
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
  3. Neither the name of mosquitto nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
  POSSIBILITY OF SUCH DAMAGE.
*/

#define _GNU_SOURCE
#include <config.h>

#include <assert.h>
#ifndef WIN32
/* Libevent */
#include <event2/event.h>
#include <event2/event_struct.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#else
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <pthread.h>

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <time_mosq.h>
#include <util_mosq.h>


extern bool flag_reload;
#ifdef WITH_PERSISTENCE
extern bool flag_db_backup;
#endif
extern bool flag_tree_print;
extern int run;
#ifdef WITH_SYS_TREE
extern int g_clients_expired;
#endif

/* static void loop_handle_errors(struct mosquitto_db *db, struct kevent *); */
static void do_disconnect(struct mosquitto_db *db, int fd);

//hack for apple
void diep(const char *s);
int conn_delete(int fd);
int loop_push_db_context_msg(void *arg);

//打开监听套接字后，就可以进入消息事件循环
int mosquitto_main_loop(struct mosquitto_db *db, int *listensock, int listensock_count, int listener_max)
{
  struct event *ev;
  pthread_t tid;
  int ret;

  //temp hack for multi listen socket
  int _listensock = listensock[0];
  listensock_count = 1;

  /* Initial libevent. */
  struct event_base *base = event_base_new();
  assert(base != NULL);

  struct mosquitto_funcs_data funcs_data = { db, base };

  //注册监听sock可读事件。也就是新连接事件
  for (int i = 0; i < listensock_count; ++i)
    {
      /* Create event. */
      ev = event_new(base, listensock[i], EV_READ|EV_PERSIST, mqtt3_socket_accept, &funcs_data);
      if(!ev){
          _mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
          return MOSQ_ERR_NOMEM;
      }
      /* Add event. */
      event_add(ev, NULL);
    }

  ret = pthread_create(&tid, NULL, loop_push_db_context_msg, db);
  if (ret != 0) {
    perror("pthread_create()");
    return 1;
  }

  event_base_dispatch(base);

  //TODO any cleanup here?
  return MOSQ_ERR_SUCCESS;
}

// worker thread
int loop_push_db_context_msg(void *arg)
{
 	int time_count;
#ifndef WIN32
	sigset_t sigblock, origsig;
#endif
	int i;
#ifdef WITH_BRIDGE
	int bridge_sock;
	int rc;
#endif
#ifndef WIN32
	sigemptyset(&sigblock);
	sigaddset(&sigblock, SIGINT);
#endif
	time_t now;
  struct mosquitto_db *db = (struct mosquitto_db *)arg;

  time_t start_time = mosquitto_time();
	time_t last_backup = mosquitto_time();
	time_t last_store_clean = mosquitto_time();


#ifdef WITH_SYS_TREE
  // TODO check this to see how send message work
  mqtt3_db_sys_update(db, db->config->sys_interval, start_time);
#endif
  mqtt3_db_message_timeout_check(db, db->config->retry_interval);

  //遍历每一个客户端连接,尝试将其加入监听队列里面
  time_count = 0;
  for(i=0; i<db->context_count; i++){
    if(db->contexts[i]){
      if(time_count > 0){
        time_count--;
      }else{
        time_count = 1000;
        now = mosquitto_time();
      }

      if(db->contexts[i]->sock != INVALID_SOCKET){
        // 处理每个客户端

        // 处理bridge情况
#ifdef WITH_BRIDGE
        if(db->contexts[i]->bridge){
          _mosquitto_check_keepalive(db->contexts[i]);
          if(db->contexts[i]->bridge->round_robin == false
             && db->contexts[i]->bridge->cur_address != 0
             && now > db->contexts[i]->bridge->primary_retry){

            /* FIXME - this should be non-blocking */
            // broker的连接策略可以看下man mosquitto.conf 的说明，比较清晰点
            if(_mosquitto_try_connect(db->contexts[i]->bridge->addresses[0].address, db->contexts[i]->bridge->addresses[0].port, &bridge_sock, NULL, true) == MOSQ_ERR_SUCCESS){
              COMPAT_CLOSE(bridge_sock);
              _mosquitto_socket_close(db->contexts[i]);
              db->contexts[i]->bridge->cur_address = db->contexts[i]->bridge->address_count-1; // 不断的去测试新bridge地址...
            }
          }
        }
#endif

        /* Local bridges never time out in this fashion. */
        if(!(db->contexts[i]->keepalive)
           || db->contexts[i]->bridge
           || now - db->contexts[i]->last_msg_in < (time_t)(db->contexts[i]->keepalive)*3/2){
          //在进入poll等待之前，先尝试将未发送的数据发送出去
          if(mqtt3_db_message_write(db->contexts[i]) == MOSQ_ERR_SUCCESS){
            // silence is god.

          }else{ //尝试发送失败，连接出问题了
            mqtt3_context_disconnect(db, db->contexts[i]);
          }
        }else{ //超过1.5倍的时间，超时关闭连接
          if(db->config->connection_messages == true){
            _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s has exceeded timeout, disconnecting.", db->contexts[i]->id);
          }
          /* Client has exceeded keepalive*1.5 */
          mqtt3_context_disconnect(db, db->contexts[i]); // 关闭连接，清空数据，后续还可以用.sock=INVALID_SOCKET
        }
      }else{

        // TODO bridge的情况
#ifdef WITH_BRIDGE
        if(db->contexts[i]->bridge){
          /* Want to try to restart the bridge connection */
          if(!db->contexts[i]->bridge->restart_t){
            db->contexts[i]->bridge->restart_t = now+db->contexts[i]->bridge->restart_timeout;
            db->contexts[i]->bridge->cur_address++;
            if(db->contexts[i]->bridge->cur_address == db->contexts[i]->bridge->address_count){
              db->contexts[i]->bridge->cur_address = 0;
            }
            if(db->contexts[i]->bridge->round_robin == false && db->contexts[i]->bridge->cur_address != 0){
              db->contexts[i]->bridge->primary_retry = now + 5;
            }
          }else{
            if(db->contexts[i]->bridge->start_type == bst_lazy && db->contexts[i]->bridge->lazy_reconnect){
              rc = mqtt3_bridge_connect(db, db->contexts[i]);
              if(rc){
                db->contexts[i]->bridge->cur_address++;
                if(db->contexts[i]->bridge->cur_address == db->contexts[i]->bridge->address_count){
                  db->contexts[i]->bridge->cur_address = 0;
                }
              }
            }
            if(db->contexts[i]->bridge->start_type == bst_automatic && now > db->contexts[i]->bridge->restart_t){
              db->contexts[i]->bridge->restart_t = 0;
              rc = mqtt3_bridge_connect(db, db->contexts[i]);
              if(rc == MOSQ_ERR_SUCCESS){

                // 这个socket事件应该加到主的base里面去吧=.=....
                printf("do something, my friends");
                /* Install time server. */
                /* ev = _mosquitto_malloc(sizeof(struct event)); */
                /* if (!ev) */
                /* { */
                // can not accept more events
                /* return MOSQ_ERR_NOMEM; */
                /* } */

                /* event_set(ev, db->contexts[i]->sock, EV_WRITE|EV_READ|EV_PERSIST, (void *) loop_handle_reads_writes, db); */
                /* event_add(ev, NULL); */

              }else{
                /* Retry later. */
                db->contexts[i]->bridge->restart_t = now+db->contexts[i]->bridge->restart_timeout;

                db->contexts[i]->bridge->cur_address++;
                if(db->contexts[i]->bridge->cur_address == db->contexts[i]->bridge->address_count){
                  db->contexts[i]->bridge->cur_address = 0;
                }
              }
            }
          }
        }else{
#endif
          //这个连接上次由于什么原因，挂了，设置了clean session，所以这里直接彻底清空其结构
          if(db->contexts[i]->clean_session == true){
            mqtt3_context_cleanup(db, db->contexts[i], true);
            db->contexts[i] = NULL;
          }else if(db->config->persistent_client_expiration > 0){
            //协议规定persistent_client的状态必须永久保存，这里避免连接永远无法删除，增加这个超时选项。
            //也就是如果一个客户端断开连接一段时间了，那么我们会主动干掉他
            /* This is a persistent client, check to see if the
             * last time it connected was longer than
             * persistent_client_expiration seconds ago. If so,
             * expire it and clean up.
             */
            if(now > db->contexts[i]->disconnect_t+db->config->persistent_client_expiration){
              _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Expiring persistent client %s due to timeout.", db->contexts[i]->id);
#ifdef WITH_SYS_TREE
              g_clients_expired++;
#endif
              db->contexts[i]->clean_session = true;
              mqtt3_context_cleanup(db, db->contexts[i], true);
              db->contexts[i] = NULL;
            }
          }
#ifdef WITH_BRIDGE
        }
#endif
      }
    }
  }// end of for loop

  /* 一些定时任务, 应该在while(run)的逻辑里面 */
#ifdef WITH_PERSISTENCE
    if(db->config->persistence && db->config->autosave_interval){
      if(db->config->autosave_on_changes){
        if(db->persistence_changes > db->config->autosave_interval){
          mqtt3_db_backup(db, false, false);
          db->persistence_changes = 0;
        }
      }else{
        if(last_backup + db->config->autosave_interval < mosquitto_time()){
          mqtt3_db_backup(db, false, false);
          last_backup = mosquitto_time();
        }
      }
    }
#endif
  if(!db->config->store_clean_interval || last_store_clean + db->config->store_clean_interval < mosquitto_time()){
    mqtt3_db_store_clean(db);
    last_store_clean = mosquitto_time();
  }
#ifdef WITH_PERSISTENCE
  if(flag_db_backup){
    mqtt3_db_backup(db, false, false);
    flag_db_backup = false;
  }
#endif
  if(flag_reload){
    _mosquitto_log_printf(NULL, MOSQ_LOG_INFO, "Reloading config.");
    mqtt3_config_read(db->config, true);
    mosquitto_security_cleanup(db, true);
    mosquitto_security_init(db, true);
    mosquitto_security_apply(db);
    mqtt3_log_init(db->config->log_type, db->config->log_dest);
    flag_reload = false;
  }
  if(flag_tree_print){
    mqtt3_sub_tree_print(&db->subs, 0);
    flag_tree_print = false;
  }

  sleep(1); // give back control
}

static void do_disconnect(struct mosquitto_db *db, int fd)
{

  int context_index=0;

  //TODO better handle context sock disconn
  for (int i = 0; i < db->context_count; ++i)
    {
      if (db->contexts[context_index]->sock == fd)
        {
          break;
        }
      context_index++;
    }

  if(db->config->connection_messages == true){
    if(db->contexts[context_index]->state != mosq_cs_disconnecting){
      _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket error on client %s, disconnecting.", db->contexts[context_index]->id);
    }else{
      _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", db->contexts[context_index]->id);
    }
  }
  mqtt3_context_disconnect(db, db->contexts[context_index]);
}

/* Error ocurred, probably an fd has been closed.
 * Loop through and check them all.
 */
/* static void loop_handle_errors(struct mosquitto_db *db, struct kevent *evlist) */
/* { */
/*   int i; */
/* //处理客户端socket的错误事件，清理资源，设置状态 */
/*   // 不会把整个结构体干掉 */
/*   for(i=0; i<db->context_count; i++) */
/*     { */
/*       if( db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET ) */
/*         { */
/*           if( evlist[i].flags & EV_ERROR){ */
/*             do_disconnect(db, ); */
/*           } */
/*         } */
/*     } */
/* } */


// 算法复杂度O(n)
 void loop_handle_reads_writes(int fd, short ev, void *arg)
 {//mosquitto_main_loop调用这里来处理客户端连接的读写事件
   int i;
   struct mosquitto_db *db = arg;
   for(i=0; i<db->context_count; i++){

     // socket可写
     if(db->contexts[i] && db->contexts[i]->sock == fd){

#ifdef WITH_TLS
       if(ev & EV_WRITE||
          db->contexts[i]->want_write ||
          (db->contexts[i]->ssl && db->contexts[i]->state == mosq_cs_new)){
#else
         if(ev & EV_WRITE){
#endif
           if(_mosquitto_packet_write(db->contexts[i])){
             if(db->config->connection_messages == true){
               if(db->contexts[i]->state != mosq_cs_disconnecting){
                 _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket write error on client %s, disconnecting.", db->contexts[i]->id);
               }else{
                 _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", db->contexts[i]->id);
               }
             }
             /* Write error or other that means we should disconnect */
             mqtt3_context_disconnect(db, db->contexts[i]);
           }
#ifdef WITH_TLS
         }
#else
       } //end of with_tls
#endif

       // socket可读
#ifdef WITH_TLS
       if(ev & EV_READ ||
          (db->contexts[i]->ssl && db->contexts[i]->state == mosq_cs_new)){
#else
         if(ev & EV_READ){
#endif
           if(_mosquitto_packet_read(db, db->contexts[i])){
             if(db->config->connection_messages == true){
               if(db->contexts[i]->state != mosq_cs_disconnecting){
                 _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket read error on client %s, disconnecting.", db->contexts[i]->id);
               }else{
                 _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", db->contexts[i]->id);
               }
             }
             /* Read error or other that means we should disconnect */
             mqtt3_context_disconnect(db, db->contexts[i]);
           }
#ifdef WITH_TLS
         }
#else
       }
#endif

       // 其他错误，直接断开连接
       if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
         if (ev & (EV_SIGNAL | EV_TIMEOUT | EV_ET))
           {
             do_disconnect(db, db->contexts[i]->sock);
           }
       }

     } //end of sock == ident
   } // end of for loop
 }

void
diep(const char *s)
{
  perror(s);
  exit(EXIT_FAILURE);
}

int
conn_delete(int fd) {
  return close(fd);
}
