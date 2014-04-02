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

int push_update_db_context(int fd, short n,struct mosquitto_funcs_data *arg);

//打开监听套接字后，就可以进入消息事件循环
int
mosquitto_main_loop(struct mosquitto_db *db, int *listensock, int listensock_count, int listener_max)
{
  struct event *ev;
  pthread_t tid;
  int ret;

  //temp hack for multi listen socket
  int _listensock = listensock[0];
  listensock_count = 1;

  /* Initial libevent. */
  struct event_base *base = event_base_new();
  struct event ev_timer;
  struct timeval tv;

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

  ev = event_new(base, -1, EV_PERSIST, push_update_db_context, &funcs_data);
  if(!ev){
    _mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
    return MOSQ_ERR_NOMEM;
  }
  // TODO better way to handle this
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  evtimer_add(ev, &tv);

  event_base_dispatch(base);

  return MOSQ_ERR_SUCCESS;
}


// TODO 拆分这里的逻辑
int
push_update_db_context(int fd, short n, struct mosquitto_funcs_data *arg)
{
 	int time_count;
	int i;
	int bridge_sock;
	int rc;

  time_t now;
  struct mosquitto_db *db = arg->db;
  struct event_base * base = arg->base;
  struct event *ev;

  time_t start_time = mosquitto_time();
	time_t last_backup = mosquitto_time();
	time_t last_store_clean = mosquitto_time();

#ifdef WITH_SYS_TREE
  // 更新sys信息，把这些信息都插入到特定的某些系统主题下，开放给订阅者
  mqtt3_db_sys_update(db, db->config->sys_interval, start_time);
#endif

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

      // 客户端在线
      if(db->contexts[i]->sock != INVALID_SOCKET){

        // 检测bridge的连接情况，超时就关闭掉
        // 并重连broker
        if(db->contexts[i]->bridge){
          _mosquitto_check_keepalive(db->contexts[i]);
          if(db->contexts[i]->bridge->round_robin == false
             && db->contexts[i]->bridge->cur_address != 0
             && now > db->contexts[i]->bridge->primary_retry){

            /* FIXME - this should be non-blocking */
            // broker的连接策略可以看下man mosquitto.conf 的说明，比较清晰点
            if(_mosquitto_try_connect(db->contexts[i]->bridge->addresses[0].address, db->contexts[i]->bridge->addresses[0].port, &bridge_sock, NULL, true) == MOSQ_ERR_SUCCESS){ //TODO 这里的逻辑好奇怪
              COMPAT_CLOSE(bridge_sock);
              _mosquitto_socket_close(db->contexts[i]);
              db->contexts[i]->bridge->cur_address = db->contexts[i]->bridge->address_count-1; // 告诉下一次连接的时候，直接连接主bridge地址上
            }
          }
        }

        // 发送堆积的消息，并清除超时的连接
        /* Local bridges never time out in this fashion. */
        if(!(db->contexts[i]->keepalive)
           || db->contexts[i]->bridge
           || now - db->contexts[i]->last_msg_in < (time_t)(db->contexts[i]->keepalive)*3/2){
          //先尝试把堆积在每个context下面的信息发送出去
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

        // 处理客户端不在线的情况

        /* start_type [ automatic | lazy | once ] */
        /* Set the start type of the bridge. This controls how the bridge starts and can be one of three types: automatic, lazy and once. Note that RSMB provides a fourth start type "manual" which isn't currently supported by mosquitto. */
        /* automatic is the default start type and means that the bridge connection will be started automatically when the broker starts and also restarted after a short delay (30 seconds) if the connection fails. */
        /* Bridges using the lazy start type will be started automatically when the number of queued messages exceeds the number set with the threshold option. It will be stopped automatically after the time set by the idle_timeout parameter. */
        /* Use this start type if you wish the connection to only be active when it is needed. */
        /* A bridge using the once start type will be started automatically when the broker starts but will not be restarted if the connection fails.' */

        // 上一次的bridge连接没建立成功
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

            // bst --> bridge start type, restart_t ==  30s
            if(db->contexts[i]->bridge->start_type == bst_automatic && now > db->contexts[i]->bridge->restart_t){
              db->contexts[i]->bridge->restart_t = 0;
              rc = mqtt3_bridge_connect(db, db->contexts[i]);
              if(rc == MOSQ_ERR_SUCCESS){

                /* Create event. */
                ev = event_new(base, db->contexts[i]->sock, EV_READ|EV_PERSIST, loop_handle_reads_writes, db);
                if(!ev){
                  /* TODO error handle in Libevent */
                  _mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
                  return MOSQ_ERR_NOMEM;
                }
                /* Add event. */
                event_add(ev, NULL);

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
          // 当前客户端不是bridge的情况

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
        }
      }
    }
  }// end of for loop

  // 检测每个客户端的msgs链表里面每个msg的超时情况，改变它们响应的状态码
  mqtt3_db_message_timeout_check(db, db->config->retry_interval);

  /* 一些定时的备份任务*/
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

}

static void do_disconnect(struct mosquitto_db *db, int fd)
{

  int context_index=0;

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

  //TODO 这里可以弄个hash tabble来O(1)的效率
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
          printf("可读\n");

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

     break; //跳出for循环

    } //end of sock == ident

  } // end of for loop

}
