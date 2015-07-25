package main

import (
        "launchpad.net/gozk"
        "log"
        "time"
)

func zkConnect(zkuri string) *zookeeper.Conn {
        zkconn, session, err := zookeeper.Dial(zkuri,5e9)
        if err != nil {
                log.Printf("zkConnect:Error: Can't connect to zookeeper: %s %s", zkuri, err.Error())
                return nil
        }
        event := <-session
        if event.State != zookeeper.STATE_CONNECTED {
                log.Printf("zkConnect:Error: Can't reach connected state: %s %v", zkuri, event)
                err = zkconn.Close()
                if err != nil {
                        log.Printf("zkConnect:Error:  Failed closing connection to zookeeper: %s %s", zkuri, err.Error())
                        return nil
                }
                return nil
        }
        return zkconn
}

func getLowestPath(mutexpath string, children []string, path string) string {
        var lowpath string = path
        for _,value := range children {
                var child string = mutexpath + "/" + value
                if lowpath > child {
                        lowpath = child
                }
        }
        return lowpath
}

func runMutexProtocol(ZkUri, ZkMutexPath string) bool {
  var path string = ZkMutexPath + "/CliedntId-"
  zkconn := zkConnect(ZkUri)
  if zkconn == nil {
     log.Printf("runMutexProtocol:Error: Can't connect: %s ", ZkUri)
     return false
  }
  defer zkconn.Close()
  npath, err := zkconn.Create(path,"",zookeeper.SEQUENCE|zookeeper.EPHEMERAL,zookeeper.WorldACL(zookeeper.PERM_ALL));
  if err != nil {
     log.Printf("runMutexProtocol:Error: Can't create path: %s %s", path, err.Error())
     return false
  }     
  for {
     children, _, err := zkconn.Children(ZkMutexPath)
     if err != nil {
        log.Printf("runMutexProtocol:Error: Can't get children: %s %s", path, err.Error())
        return false
     }
     lowpath := getLowestPath(ZkMutexPath,children,npath)
     if lowpath == npath {
        log.Printf("runMutexProtocol:Info: I got the lock  %s %s %v",npath, lowpath, children)
        // do in critical section
     } else {
        log.Printf("runMutexProtocol:Info: waiting for the lock. This may take for ever  %s %s %v",npath, lowpath, children)
         _, watch, err := zkconn.ExistsW(lowpath)
        if err != nil {
             log.Printf("runMutexProtocol:Error: Error getting watch on %s %s",lowpath, err.Error())
        } else {
             event := <-watch
             if !event.Ok() {
                  log.Printf("runMutexProtocol:Error: Error watching on %v %s",event, err.Error())
             }
        }
     }
  }
}

func main() {
 var ZkUri string = "127.0.0.1:2308"
 var ZkMutexPath string = "/testpath/_mutex_"
 for {
    if !runMutexProtocol(ZkUri, ZkMutexPath) {
       log.Printf("Runforever:Fatal: Mutex Protocal Failing")
    }
    time.Sleep(1 * time.Minute)
 }
}
