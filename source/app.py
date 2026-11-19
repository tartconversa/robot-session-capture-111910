"""多通道示教的流式通知、范围原件和授权来源。"""
import time
from source.common import *

class Site(Provider):
    def initial(self):return {'revision':1,'ended':True,'masked':False,'conflict':False,'unavailable':False,'epoch':1,'grant_ms':[0,2000]}
    def event(self,d):
        e=d['event']
        if e=='mask':self.state['masked']=True
        elif e=='grant':self.state['grant_ms']=d.get('session_ms',[0,1000])
        elif e=='conflict':self.state['conflict']=True
        elif e=='live':self.state['ended']=False
        elif e=='end':self.state['ended']=True
        elif e=='unavailable':self.state['unavailable']=True
        elif e=='reconnect':self.state['epoch']+=1
        else:return Reply({'error':'unknown_event'},400)
        self.state['revision']+=1;return {'ok':True}
    def original(self,channel,seq):
        if channel=='camera':return b'P6\n2 1\n255\n'+bytes([seq,0,0,0,seq,0])
        return (channel+':'+str(seq)).encode()+b'\x00\xff'
    def descriptor(self,ch,seq,epoch):
        raw=self.original(ch,seq)
        return {'session':'demo-A','action_type':'pick','channel':ch,'epoch':epoch,'seq':seq,'clock':seq*{'action':10,'camera':30,'annotation':100}[ch],'sha256':digest(raw),'url':f'/original?session=demo-A&channel={ch}&epoch={epoch}&seq={seq}'}
    def manifest(self):
        s=self.state;e=s['epoch']
        return {'session':'demo-A','revision':s['revision'],'ended':s['ended'],'channels':{c:{'epoch':e,'from':1,'to':3} for c in ['action','camera','annotation']},'anchors':[{'action_seq':1,'camera_seq':1,'annotation_seq':1,'session_ms':0},{'action_seq':3,'camera_seq':3,'annotation_seq':3,'session_ms':2000}],'grant':{'action_types':['pick'],'session_ms':s['grant_ms']},'redactions':[{'channel':'camera','epoch':e,'seq':[2,2]}] if s['masked'] else []}
    def get(self,p,q,h):
        if p=='/':return web(page('示教来源','<p>会话 demo-A；实时通知 /events；完整声明 /manifest；按通道补取 /backfill。</p>'))
        if p=='/manifest':return self.manifest()
        if p=='/events':
            e=self.state['epoch'];events=[{'type':'manifest','value':self.manifest()}]
            for c in ['action','camera','annotation']:
                events.extend({'type':'fragment','value':self.descriptor(c,i,e)} for i in [1,3,1])
            if self.state['conflict']:
                d=self.descriptor('camera',1,e);d['sha256']=digest(b'conflicting-original');events.append({'type':'fragment','value':d})
            def chunks():
                for i,event in enumerate(events):
                    yield ('id: '+str(i)+'\nevent: '+event['type']+'\ndata: '+dump(event['value'])+'\n\n').encode();time.sleep(.025)
            return StreamReply(chunks())
        if p=='/backfill':
            if self.state['unavailable'] and q['channel']=='camera':return Reply({'error':'source_gap'},503)
            return {'items':[self.descriptor(q['channel'],i,int(q['epoch'])) for i in range(max(1,int(q.get('from',1))),min(3,int(q.get('to',3)))+1)]}
        if p=='/original':
            if q['session']!='demo-A' or int(q['epoch'])!=self.state['epoch']:return Reply({'error':'wrong_epoch'},409)
            if q['channel']=='camera' and int(q['seq'])==2 and self.state['masked']:return Reply({'error':'redacted'},451)
            raw=self.original(q['channel'],int(q['seq']));return Reply(raw,mime='application/octet-stream',headers={'X-Content-SHA256':digest(raw)})
        return super().get(p,q,h)

if __name__=='__main__':main(Site)
