import { useState, useEffect, useRef, useCallback } from 'react';

export function jobStatus(job) {
  if (!job) return {state:'IDLE',jobId:null,message:''};
  const states={pending:'PREPARING',processing:job.phase==='waiting' || job.phase==='requesting' ? 'PREPARING' : 'STAGING',completed:'COMMITTED',failed:'FAILED'};
  return {...job,state:states[job.status] || 'IDLE',message:job.error || (job.status==='completed' ? `Imported ${job.insertedCount || 0} trades; ${job.duplicatesSkipped || 0} duplicates skipped` : job.phase==='waiting' ? 'Waiting for external extraction completion' : job.phase || ''),startedAt:job.startedAt ? new Date(job.startedAt).getTime() : Date.now()};
}
async function request(url,options={}) {
  const response=await fetch(url,{signal:AbortSignal.timeout(20000),...options});
  const payload=await response.json();
  if (!response.ok || payload.success===false) throw new Error(typeof payload.error==='string' ? payload.error : 'Request failed');
  return payload;
}
export function useTradePulseSSE() {
  const [market,setMarket]=useState([]);
  const [orders,setOrders]=useState([]);
  const [trades,setTrades]=useState([]);
  const [pagination,setPagination]=useState({page:1,limit:30,total:0,totalPages:1});
  const [stats,setStats]=useState({totalTrades:0,totalVolume:0,buyTrades:0,sellTrades:0});
  const [importStatus,setImportStatus]=useState(jobStatus(null));
  const [isConnected,setIsConnected]=useState(false);
  const [isLoading,setIsLoading]=useState(true);
  const [error,setError]=useState(null);
  const currentPageRef=useRef(1);
  const lastEventIdRef=useRef('0');
  const revision=useRef(0);
  const jobRevision=useRef(0);
  const refreshRunning=useRef(false);
  const refreshPending=useRef(false);
  const fetchPage=useCallback(async (page=1)=> {
    const ticket=++revision.current;
    currentPageRef.current=page;setIsLoading(true);
    try {
      let result=await request(`/api/trades?page=${page}&limit=30`);
      if (page>result.pagination.totalPages) {
        page=result.pagination.totalPages;
        result=await request(`/api/trades?page=${page}&limit=30`);
      }
      if (ticket!==revision.current) return;
      currentPageRef.current=page;setTrades(result.data);setPagination(result.pagination);setError(null);
    } catch(error) { if (ticket===revision.current) setError(error.message); }
    finally { if (ticket===revision.current) setIsLoading(false); }
  },[]);
  const refreshStats=useCallback(async ()=> {
    try { setStats((await request('/api/trades/stats')).data); } catch(error) { setError(error.message); }
  },[]);
  const refreshData=useCallback(async ()=> {
    refreshPending.current=true;
    if (refreshRunning.current) return;
    refreshRunning.current=true;
    try {
      while (refreshPending.current) {
        refreshPending.current=false;
        await Promise.allSettled([fetchPage(currentPageRef.current),refreshStats(),
          request('/api/market').then(data=>setMarket(data.data)),request('/api/orders').then(data=>setOrders(data.data))]);
      }
    } finally { refreshRunning.current=false; }
  },[fetchPage,refreshStats]);
  const resetDatabase=useCallback(async ()=> {
    const result=await request('/api/reset',{method:'POST'});currentPageRef.current=1;await refreshData();return result;
  },[refreshData]);
  const refreshSource=useCallback(async id=> {
    const result=await request(`/api/pulls/${id}/refresh`,{method:'POST'});setImportStatus(jobStatus(result.job));return result;
  },[]);
  const retryPull=useCallback(async id=> {
    const result=await request(`/api/pulls/${id}/retry`,{method:'POST'});setImportStatus(jobStatus(result.data));return result;
  },[]);
  useEffect(()=> {
    let stream,reconnectTimer,disposed=false,reconnectDelay=1000;
    refreshData();
    function connect() {
      if (disposed) return;
      stream=new EventSource(`/api/events?cursor=${lastEventIdRef.current}`);
      const reconnect=(scheduled=false)=> {
        if (disposed) return;
        setIsConnected(false);stream.close();clearTimeout(reconnectTimer);
        const delay=scheduled ? 1000 : reconnectDelay;
        if (!scheduled) reconnectDelay=Math.min(reconnectDelay*2,30000);
        reconnectTimer=setTimeout(connect,delay);
      };
      stream.onerror=()=>reconnect();stream.addEventListener('reconnect',()=>reconnect(true));
      stream.addEventListener('connected',event=> {
        try {
          const snapshot=JSON.parse(event.data);
          reconnectDelay=1000;
          jobRevision.current++;
          setStats(snapshot.stats);setImportStatus({...jobStatus(snapshot.importJob),completionMode:snapshot.completionMode});setIsConnected(true);
          if (snapshot.eventId!==lastEventIdRef.current) refreshData();
          lastEventIdRef.current=snapshot.eventId;
        } catch(error) { setError(error.message); }
      });
      const onEvent=event=> {
        if (event.lastEventId && BigInt(event.lastEventId)<=BigInt(lastEventIdRef.current)) return;
        if (event.lastEventId) lastEventIdRef.current=event.lastEventId;
        try {
          const payload=JSON.parse(event.data);
          const jobTicket=++jobRevision.current;
          if (['import_completed','database_reset','legacy_imported','order_saved'].includes(event.type)) refreshData();
          if (event.type==='import_started') {
            request(`/api/pulls/${payload.jobId}`).then(result=> { if (!disposed && jobTicket===jobRevision.current) setImportStatus(jobStatus(result.data)); }).catch(error=>setError(error.message));
          } else if (event.type.startsWith('import_')) {
            setImportStatus(previous=>jobStatus({...previous,...payload,status:payload.status || previous.status,
              startedAt:previous.startedAt,recordsReceived:payload.recordsReceived ?? previous.recordsReceived}));
          }
        } catch(error) { setError(error.message); }
      };
      for (const type of ['import_started','import_progress','import_completed','import_failed','database_reset','legacy_imported','order_saved']) stream.addEventListener(type,onEvent);
    }
    connect();
    return ()=> { disposed=true;clearTimeout(reconnectTimer);stream?.close(); };
  },[refreshData]);
  return {market,orders,trades,pagination,stats,importStatus,setImportStatus,isConnected,isLoading,error,
    fetchPage,refreshStats,resetDatabase,refreshSource,retryPull};
}
