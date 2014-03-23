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
#include <poll.h>
#else
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

// hack for apple
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <stdlib.h>

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <time_mosq.h>
#include <util_mosq.h>

#ifndef POLLRDHUP
/* Ignore POLLRDHUP flag on systems where it doesn't exist. */
#define POLLRDHUP 0
#endif

extern bool flag_reload;
#ifdef WITH_PERSISTENCE
extern bool flag_db_backup;
#endif
extern bool flag_tree_print;
extern int run;
#ifdef WITH_SYS_TREE
extern int g_clients_expired;
#endif

static void loop_handle_errors(struct mosquitto_db *db, struct pollfd *pollfds);
static void loop_handle_reads_writes(struct mosquitto_db *db, struct kevent *chlist);
//hack for apple
void diep(const char *s);

//打开监听套接字后，就可以进入消息事件循环
int mosquitto_main_loop(struct mosquitto_db *db, int *listensock, int listensock_count, int listener_max)
{
	time_t start_time = mosquitto_time();
	time_t last_backup = mosquitto_time();
	time_t last_store_clean = mosquitto_time();
	time_t now;
	int time_count;
	int fdcount;
#ifndef WIN32
	sigset_t sigblock, origsig;
#endif
	int i;
	struct pollfd *pollfds = NULL;
	int pollfd_count = 0;
	int pollfd_index;
#ifdef WITH_BRIDGE
	int bridge_sock;
	int rc;
#endif
#ifndef WIN32
	sigemptyset(&sigblock);
	sigaddset(&sigblock, SIGINT);
#endif

  // hack for apple
  struct kevent *chlist;
  struct kevent *evlist;
  int kevent_index,kq;

  // 用poll来做server端的轮询
  // 每次用poll之前，都要准备好pollfds数组
	while(run){
#ifdef WITH_SYS_TREE
		mqtt3_db_sys_update(db, db->config->sys_interval, start_time);
#endif

		if(listensock_count + db->context_count > pollfd_count || !pollfds){
			pollfd_count = listensock_count + db->context_count;

      /* pollfds = _mosquitto_realloc(pollfds, sizeof(struct pollfd)*pollfd_count); */
			chlist = _mosquitto_malloc(sizeof(struct kevent)*pollfd_count);
			evlist = _mosquitto_malloc(sizeof(struct kevent)*pollfd_count);

			if(!chlist||!evlist){
				_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
				return MOSQ_ERR_NOMEM;
			}
		}

		memset(chlist, -1, sizeof(struct kevent)*pollfd_count);
		memset(evlist, -1, sizeof(struct kevent)*pollfd_count);

    //注册监听sock的pollfd可读事件。也就是新连接事件
    for (int i = 0; i < listensock_count; ++i)
      {
        chlist[i].ident = listensock[i];
        chlist[i].filter = EVFILT_READ;
        chlist[i].flags = EV_ADD | EV_ENABLE;
      }
    kevent_index = listensock_count;

		/* pollfd_index = 0; */
		/* for(i=0; i<listensock_count; i++){ */
		/* 	pollfds[pollfd_index].fd = listensock[i]; */
		/* 	pollfds[pollfd_index].events = POLLIN; */
		/* 	pollfds[pollfd_index].revents = 0; */
		/* 	pollfd_index++; */
		/* } */

    //遍历每一个客户端连接,尝试将其加入chlist数组中。
    // 而且处理一些超时的socket连接等等
		time_count = 0;
		for(i=0; i<db->context_count; i++){

      // contexts[i]存在的情况下再往下走
			if(db->contexts[i]){
				if(time_count > 0){
					time_count--;
				}else{
					time_count = 1000;
					now = mosquitto_time();
				}
        // FIXME context信息要改下这里的index信息
				db->contexts[i]->pollfd_index = -1;

				if(db->contexts[i]->sock != INVALID_SOCKET){
          // 处理每个客户端

          //处理bridge情况
#ifdef WITH_BRIDGE
					if(db->contexts[i]->bridge){
						_mosquitto_check_keepalive(db->contexts[i]);
						if(db->contexts[i]->bridge->round_robin == false
               && db->contexts[i]->bridge->cur_address != 0
               && now > db->contexts[i]->bridge->primary_retry){

							/* FIXME - this should be non-blocking */
							if(_mosquitto_try_connect(db->contexts[i]->bridge->addresses[0].address, db->contexts[i]->bridge->addresses[0].port, &bridge_sock, NULL, true) == MOSQ_ERR_SUCCESS){
								COMPAT_CLOSE(bridge_sock);
								_mosquitto_socket_close(db->contexts[i]);
								db->contexts[i]->bridge->cur_address = db->contexts[i]->bridge->address_count-1;
							}
						}
					}
#endif

					/* Local bridges never time out in this fashion. */
					if(!(db->contexts[i]->keepalive)
             || db->contexts[i]->bridge
             || now - db->contexts[i]->last_msg_in < (time_t)(db->contexts[i]->keepalive)*3/2){ //处理未超时的情况
            //在进入poll等待之前，先尝试将未发送的数据发送出去
						if(mqtt3_db_message_write(db->contexts[i]) == MOSQ_ERR_SUCCESS){
              chlist[kevent_index].ident = db->contexts[i]->sock;
              chlist[kevent_index].filter = EVFILT_READ;
              chlist[kevent_index].flags = EV_ADD | EV_ENABLE;
              if(db->contexts[i]->current_out_packet){
                chlist[kevent_index].filter |= EVFILT_WRITE;
              }
              // 后面在handle_read_write的方法里面需要用到
              db->contexts[i]->pollfd_index = kevent_index;
              kevent_index++;

              /* pollfds[pollfd_index].fd = db->contexts[i]->sock; */
							/* pollfds[pollfd_index].events = POLLIN | POLLRDHUP; */
							/* pollfds[pollfd_index].revents = 0; */
							/* if(db->contexts[i]->current_out_packet){ */
              /* pollfds[pollfd_index].events |= POLLOUT; */
							/* } */
							/* db->contexts[i]->pollfd_index = pollfd_index; */
							/* pollfd_index++; */

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

          // contexts的socket是invalid的情况下
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

                  chlist[kevent_index].ident = db->contexts[i]->sock;
                  chlist[kevent_index].filter = EVFILT_READ;
                  chlist[kevent_index].flags = EV_ADD | EV_ENABLE;
                  if(db->contexts[i]->current_out_packet){
                    chlist[kevent_index].filter |= EVFILT_WRITE;
                  }
									db->contexts[i]->pollfd_index = kevent_index;
                  kevent_index++;

									/* pollfds[pollfd_index].fd = db->contexts[i]->sock; */
									/* pollfds[pollfd_index].events = POLLIN | POLLRDHUP; */
									/* pollfds[pollfd_index].revents = 0; */
									/* if(db->contexts[i]->current_out_packet){ */
                  /* pollfds[pollfd_index].events |= POLLOUT; */
									/* } */
									/* db->contexts[i]->pollfd_index = pollfd_index; */
									/* pollfd_index++; */

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
					}else{ //bridge为false的情况下是什么情况？
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
			} //end of if db->context[i]
		} //end of db->context_count

		mqtt3_db_message_timeout_check(db, db->config->retry_interval);

    // 这里开始事件循环了
#ifndef WIN32
		sigprocmask(SIG_SETMASK, &sigblock, &origsig);
		/* fdcount = poll(pollfds, pollfd_index, 100); */
    if((kq = kqueue()) == -1)
      diep("kqueue()");
    fdcount = kevent(kq, chlist, kevent_index, evlist, kevent_index, NULL);
		sigprocmask(SIG_SETMASK, &origsig, NULL);
#else
		fdcount = WSAPoll(pollfds, pollfd_index, 100);
#endif

		if(fdcount == -1){
			/* loop_handle_errors(db, pollfds); */
      diep("opps,there is errors");
		}else{
      //TODO 2014.03.24 这里的socket读写事件有点问题，整个的事件kqueue事件循环逻辑要调整下....LOL
			loop_handle_reads_writes(db, evlist);

			for(i=0; i<listensock_count; i++){
				/* if(pollfds[i].revents & (POLLIN | POLLPRI)){ */
				if(evlist[i].flags & EV_EOF){
          diep("read direction of socket has shutdown.");
        }
        if(evlist[i].flags & EV_ERROR){
          fprintf(stderr, "EVE_ERROR: %s\n", strerror(evlist[i].data));
          exit(EXIT_FAILURE);
        }
        while(mqtt3_socket_accept(db, listensock[i]) != -1){ //TODO 查看下这里的接收新的socket请求的实现
        }
      }
    }

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
  if(evlist) _mosquitto_free(evlist);
  if(chlist) _mosquitto_free(chlist);
  return MOSQ_ERR_SUCCESS;
}

static void do_disconnect(struct mosquitto_db *db, int context_index)
{
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
static void loop_handle_errors(struct mosquitto_db *db, struct pollfd *pollfds)
{
  int i;

  // 处理socket错误事件，清理资源，设置状态
  // 不会把整个结构体干掉
  for(i=0; i<db->context_count; i++){
    if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
      if(pollfds[db->contexts[i]->pollfd_index].revents & (POLLHUP | POLLRDHUP | POLLERR | POLLNVAL)){
        do_disconnect(db, i);
      }
    }
  }
}


// 算法复杂度O(n)
static void loop_handle_reads_writes(struct mosquitto_db *db, struct kevent *evlist)
{//mosquitto_main_loop调用这里来处理客户端连接的读写事件

  int i;

  for(i=0; i<db->context_count; i++){

    // socket可写
    if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
      /* assert(evlist[db->contexts[i]->pollfd_index].fd == db->contexts[i]->sock); */
      assert(evlist[db->contexts[i]->pollfd_index].ident == db->contexts[i]->sock);
#ifdef WITH_TLS
      /* if(pollfds[db->contexts[i]->pollfd_index].revents & POLLOUT || */
      if(evlist[db->contexts[i]->pollfd_index].flags & EVFILT_WRITE||
         db->contexts[i]->want_write ||
         (db->contexts[i]->ssl && db->contexts[i]->state == mosq_cs_new)){
#else
        if(evlist[db->contexts[i]->pollfd_index].flags & EVFILT_WRITE){
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
        }
      }
      // socket可读
      if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
        assert(evlist[db->contexts[i]->pollfd_index].ident == db->contexts[i]->sock);
#ifdef WITH_TLS
        if(evlist[db->contexts[i]->pollfd_index].flags & EVFILT_READ ||
           (db->contexts[i]->ssl && db->contexts[i]->state == mosq_cs_new)){
#else
          if(evlist[db->contexts[i]->pollfd_index].flags & EVFILT_READ){
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
          }
        }
        // 其他错误，直接断开连接
        if(db->contexts[i] && db->contexts[i]->sock != INVALID_SOCKET){
          if(evlist[db->contexts[i]->pollfd_index].flags & (EV_ERROR)){
            do_disconnect(db, i);
          }
        }
      }
    }

    void diep(const char *s)
    {
      perror(s);
      exit(EXIT_FAILURE);
    }
