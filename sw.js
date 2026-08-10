/* CARTAMAC rev148 - cache seguro + Firebase Cloud Messaging */
importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-messaging-compat.js');

const CACHE_NAME='cartamac-v149-ios-push';
const APP_FILES=['./','./index.html','./manifest.json','./cartamac-logo.png'];

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
  event.waitUntil(caches.open(CACHE_NAME).then(cache=>cache.addAll(APP_FILES)).then(()=>self.skipWaiting()));
});

self.addEventListener('activate',event=>{
  event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(key=>key!==CACHE_NAME).map(key=>caches.delete(key)))).then(()=>self.clients.claim()));
});

self.addEventListener('fetch',event=>{
  if(event.request.method!=='GET'||new URL(event.request.url).origin!==self.location.origin)return;
  if(event.request.mode==='navigate'){
    event.respondWith(fetch(event.request).then(response=>{const copy=response.clone();caches.open(CACHE_NAME).then(cache=>cache.put('./index.html',copy));return response;}).catch(()=>caches.match('./index.html')));
    return;
  }
  event.respondWith(caches.match(event.request).then(cached=>cached||fetch(event.request)));
});

messaging.onBackgroundMessage(payload=>{
  if(payload.notification)return;
  const data=payload.data||{};
  return self.registration.showNotification(data.title||'CARTAMAC',{
    body:data.body||'Há uma nova atualização no cronograma.',
    icon:'./cartamac-logo.png',badge:'./cartamac-logo.png',
    tag:data.notificationId||'cartamac-cronograma',renotify:true,
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
