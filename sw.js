/* CARTAMAC rev166 - HTML sempre atualizado e limpeza dos caches antigos */
importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-messaging-compat.js');

const CACHE_NAME='cartamac-v166-sem-cache-html';
const STATIC_FILES=['./manifest.json','./cartamac-logo.png'];

firebase.initializeApp({
  apiKey:'AIzaSyAlSFh4QVAZd2eIIhmJXBrT8yhIiH92pkM',
  authDomain:'cartamac-cronogramas.firebaseapp.com',
  projectId:'cartamac-cronogramas',
  storageBucket:'cartamac-cronogramas.firebasestorage.app',
  messagingSenderId:'439003938929',
  appId:'1:439003938929:web:ec76a3c650efcc63ba9bd0'
});

const messaging=firebase.messaging();

self.addEventListener('install',event=>{
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache=>Promise.all(STATIC_FILES.map(file=>cache.add(file).catch(()=>null))))
      .then(()=>self.skipWaiting())
  );
});

self.addEventListener('activate',event=>{
  event.waitUntil((async()=>{
    const keys=await caches.keys();
    await Promise.all(
      keys
        .filter(key=>key.startsWith('cartamac-')&&key!==CACHE_NAME)
        .map(key=>caches.delete(key))
    );
    await self.clients.claim();
  })());
});

self.addEventListener('message',event=>{
  if(event.data?.type==='SKIP_WAITING')self.skipWaiting();
});

self.addEventListener('fetch',event=>{
  if(event.request.method!=='GET')return;
  const url=new URL(event.request.url);
  if(url.origin!==self.location.origin)return;

  // Nunca guarda index.html nem navegações. Assim uma nova revisão publicada
  // não pode ser substituída por uma cópia antiga do Service Worker.
  if(event.request.mode==='navigate'||url.pathname.endsWith('/index.html')){
    event.respondWith(fetch(event.request,{cache:'no-store'}));
    return;
  }

  // Manifesto e logo podem usar cache; os demais recursos seguem pela rede.
  if(STATIC_FILES.some(file=>url.pathname.endsWith(file.replace('./','/')))){
    event.respondWith(
      caches.match(event.request).then(cached=>cached||fetch(event.request).then(response=>{
        if(response.ok){
          const copy=response.clone();
          caches.open(CACHE_NAME).then(cache=>cache.put(event.request,copy));
        }
        return response;
      }))
    );
    return;
  }

  event.respondWith(fetch(event.request));
});

messaging.onBackgroundMessage(payload=>{
  if(payload.notification)return;
  const data=payload.data||{};
  return self.registration.showNotification(data.title||'CARTAMAC',{
    body:data.body||'Há uma nova atualização no cronograma.',
    icon:'./cartamac-logo.png',
    badge:'./cartamac-logo.png',
    tag:data.notificationId||'cartamac-cronograma',
    renotify:true,
    data:{url:data.url||'./'}
  });
});

self.addEventListener('notificationclick',event=>{
  event.notification.close();
  const target=new URL(event.notification.data?.url||'./',self.location.origin).href;
  event.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(windows=>{
    const open=windows.find(client=>client.url.startsWith(self.location.origin));
    if(open){open.navigate(target);return open.focus();}
    return clients.openWindow(target);
  }));
});
