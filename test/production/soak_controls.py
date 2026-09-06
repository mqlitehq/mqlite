#!/usr/bin/env python3
"""Exercise the actual soak oracles with valid and deliberately invalid inputs.

No broker runs here. A temporary copy retains the three production runner source
files and replaces only main's entry-point name, then adds a control entry point.
The resulting Go program calls receive/reconcile/checkRetained/verified/audit
directly. Synthetic history fixtures are controls, never soak acceptance evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

CONTROL = r'''
package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    "reflect"
    "strings"
    "time"

    mq "github.com/mqlitehq/mqlite"
)

type controlAPI struct {
    queueAPI
    rows []*mq.Message
    retained []*mq.PeekedMessage
}
func (a *controlAPI) Receive(ctx context.Context, q string, opts ...mq.RecvOpts) ([]*mq.Message, error) {
    rows := a.rows
    a.rows = nil
    return rows, ctx.Err()
}
func (a *controlAPI) Peek(context.Context, string, ...mq.PeekOpts) ([]*mq.PeekedMessage, error) {
    return a.retained, nil
}
func (a *controlAPI) Stats(context.Context, string) (mq.Metrics, error) {
    return mq.Metrics{}, nil
}

var controlRoot string
var controls []map[string]any

func recordControl(name, surface string, err error, reject bool) {
    ok := (err != nil) == reject
    message := ""
    if err != nil { message = err.Error() }
    controls = append(controls, map[string]any{"name": name, "surface": surface,
        "expected_rejection": reject, "passed": ok, "error": message})
    if !ok { panic(fmt.Sprintf("control %s: expected rejection=%v, got %v", name, reject, err)) }
}

func fixture(name string) (*batch, *controlAPI, []*mq.Message) {
    dir := filepath.Join(controlRoot, name)
    if err := os.MkdirAll(filepath.Join(dir, "pending"), 0700); err != nil { panic(err) }
    p := makePlan("0123456789abcdef0123456789abcdef", 0, 0, time.Now().Add(-time.Second).UnixMilli())
    ledger, err := beginBatch(dir, p)
    if err != nil { panic(err) }
    api := &controlAPI{}
    b := &batch{plan: p, ledger: ledger, api: api,
        acked: map[string]bool{}, owners: map[string]ownership{}, sequences: map[string]string{},
        reset: map[string]bool{}, done: map[string]bool{}, hits: map[string]bool{}}
    q := queue(p.Seed, "ordinary")
    rows := make([]*mq.Message, len(p.Inputs))
    for i, in := range p.Inputs {
        m := (&mq.Client{}).Message(q, int64(i+1), fmt.Sprintf("token-%d", i))
        m.MessageID, m.Body = in.ID, append([]byte(nil), in.Body...)
        m.GroupID, m.Subject, m.CorrelationID = in.Group, in.Subject, in.Correlation
        m.ReplyTo, m.ContentType = in.Reply, in.ContentType
        m.Properties = map[string]string{}
        for k, v := range in.Properties { m.Properties[k] = v }
        m.DeliveryCount = 1
        m.EnqueuedAt, m.LockedUntil = time.UnixMilli(p.CreatedAt+1), time.Now().Add(10*time.Second)
        rows[i] = m
        b.acked[in.ID] = true
        b.owners[key(q,in.ID)] = ownership{sequence: m.SequenceNumber}
    }
    api.rows = rows
    return b, api, rows
}

func withToken(m *mq.Message, token string) *mq.Message {
    out := (&mq.Client{}).Message("control", m.SequenceNumber, token)
    saved := *m
    savedToken := *out
    savedToken.Body, savedToken.MessageID = saved.Body, saved.MessageID
    savedToken.GroupID, savedToken.CorrelationID = saved.GroupID, saved.CorrelationID
    savedToken.ReplyTo, savedToken.Subject = saved.ReplyTo, saved.Subject
    savedToken.ContentType, savedToken.Properties = saved.ContentType, saved.Properties
    savedToken.DeliveryCount, savedToken.EnqueuedAt = saved.DeliveryCount, saved.EnqueuedAt
    savedToken.LockedUntil = saved.LockedUntil
    return &savedToken
}

func receiveControls() {
    mutations := []struct{
        name string
        apply func(*batch, *controlAPI, []*mq.Message)
        replay, atMost, reject bool
    }{
        {"valid-fresh", func(*batch,*controlAPI,[]*mq.Message){}, false,false,false},
        {"body-first-byte", func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Body[0]^=1},false,false,true},
        {"body-middle-byte",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Body[len(m[0].Body)/2]^=1},false,false,true},
        {"body-last-byte",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Body[len(m[0].Body)-1]^=1},false,false,true},
        {"body-truncated",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Body=m[0].Body[:len(m[0].Body)-1]},false,false,true},
        {"message-id",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].MessageID+="x"},false,false,true},
        {"group-id",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].GroupID+="x"},false,false,true},
        {"subject",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Subject+="x"},false,false,true},
        {"correlation",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].CorrelationID+="x"},false,false,true},
        {"reply",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].ReplyTo+="x"},false,false,true},
        {"content-type",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].ContentType+="x"},false,false,true},
        {"property-value",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Properties["unicode"]="cafe"},false,false,true},
        {"property-missing",func(_ *batch,_ *controlAPI,m []*mq.Message){delete(m[0].Properties,"index")},false,false,true},
        {"property-extra",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].Properties["extra"]="x"},false,false,true},
        {"sequence-zero",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].SequenceNumber=0},false,false,true},
        {"sequence-changed",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].SequenceNumber+=100},false,false,true},
        {"sequence-collision",func(b *batch,_ *controlAPI,m []*mq.Message){
            m[1].SequenceNumber=m[0].SequenceNumber
            q:=queue(b.plan.Seed,"ordinary"); delete(b.owners,key(q,m[1].MessageID))
        },false,false,true},
        {"delivery-count",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].DeliveryCount++},false,false,true},
        {"enqueue-zero",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].EnqueuedAt=time.Time{}},false,false,true},
        {"enqueue-before-send",func(b *batch,_ *controlAPI,m []*mq.Message){m[0].EnqueuedAt=time.UnixMilli(b.plan.CreatedAt-1)},false,false,true},
        {"enqueue-in-future",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].EnqueuedAt=time.Now().Add(time.Second)},false,false,true},
        {"token-missing",func(_ *batch,a *controlAPI,m []*mq.Message){a.rows[0]=withToken(m[0],"")},false,false,true},
        {"expired-lease",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].LockedUntil=time.Now().Add(-time.Millisecond)},false,false,true},
        {"duplicate-delivery",func(_ *batch,a *controlAPI,m []*mq.Message){a.rows[1]=m[0]},false,false,true},
        {"missing-delivery",func(_ *batch,a *controlAPI,m []*mq.Message){a.rows=m[:3]},false,false,true},
        {"extra-delivery",func(_ *batch,a *controlAPI,m []*mq.Message){a.rows=append(m,m[0])},false,false,true},
        {"valid-at-most-once",func(_ *batch,a *controlAPI,m []*mq.Message){
            for i:=range m {a.rows[i]=withToken(m[i],""); a.rows[i].LockedUntil=time.Time{}}
        },false,true,false},
        {"at-most-once-token",func(*batch,*controlAPI,[]*mq.Message){},false,true,true},
        {"at-most-once-lease",func(_ *batch,a *controlAPI,m []*mq.Message){
            for i:=range m {a.rows[i]=withToken(m[i],"")}
        },false,true,true},
        {"valid-attempt-replay",func(*batch,*controlAPI,[]*mq.Message){},true,false,false},
        {"replay-token",func(_ *batch,a *controlAPI,m []*mq.Message){a.rows[0]=withToken(m[0],"changed")},true,false,true},
        {"replay-deadline",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].LockedUntil=m[0].LockedUntil.Add(time.Second)},true,false,true},
        {"replay-enqueue-time",func(_ *batch,_ *controlAPI,m []*mq.Message){m[0].EnqueuedAt=m[0].EnqueuedAt.Add(time.Second)},true,false,true},
        {"redrive-reuses-token",func(b *batch,_ *controlAPI,m []*mq.Message){
            q:=queue(b.plan.Seed,"ordinary")
            b.owners[key(q,m[0].MessageID)]=ownership{m[0].SequenceNumber,3,m[0].LockToken(),m[0].EnqueuedAt,m[0].LockedUntil}
            b.reset[key(q,m[0].MessageID)]=true
        },false,false,true},
        {"redelivery-without-count-increment",func(b *batch,_ *controlAPI,m []*mq.Message){
            q:=queue(b.plan.Seed,"ordinary")
            b.owners[key(q,m[0].MessageID)]=ownership{m[0].SequenceNumber,1,"older-token",m[0].EnqueuedAt,m[0].LockedUntil}
        },false,false,true},
        {"redelivery-enqueue-time",func(b *batch,_ *controlAPI,m []*mq.Message){
            q:=queue(b.plan.Seed,"ordinary")
            b.owners[key(q,m[0].MessageID)]=ownership{m[0].SequenceNumber,3,"older-token",m[0].EnqueuedAt.Add(-time.Second),m[0].LockedUntil}
            b.reset[key(q,m[0].MessageID)]=true
        },false,false,true},
    }
    for _, c := range mutations {
        b, api, rows := fixture(c.name)
        q:=queue(b.plan.Seed,"ordinary")
        if c.replay {
            for _,m:=range rows {
                b.owners[key(q,m.MessageID)]=ownership{m.SequenceNumber,m.DeliveryCount,m.LockToken(),m.EnqueuedAt,m.LockedUntil}
            }
        }
        c.apply(b,api,rows)
        ctx,cancel:=context.WithTimeout(context.Background(),50*time.Millisecond)
        opts:=mq.RecvOpts{AtMostOnce:c.atMost}
        if c.replay { opts.Attempt="original-attempt" }
        _,err:=b.receive(ctx,q,[]int{0,1,2,3},1,opts,c.replay)
        cancel()
        if closeErr:=b.ledger.close(); closeErr!=nil {panic(closeErr)}
        recordControl(c.name,"receive online full-content/ownership oracle",err,c.reject)
    }
    // This golden makes adding an SDK delivery field require a deliberate oracle review.
    var fields []string
    typ:=reflect.TypeOf(mq.Message{})
    for i:=0;i<typ.NumField();i++ {f:=typ.Field(i); if f.IsExported(){fields=append(fields,f.Name)}}
    want:=[]string{"SequenceNumber","Body","MessageID","GroupID","CorrelationID","ReplyTo","Subject","ContentType","Properties","DeliveryCount","EnqueuedAt","LockedUntil"}
    var err error
    if !reflect.DeepEqual(fields,want){err=fmt.Errorf("delivered field surface changed: %v",fields)}
    recordControl("delivery-field-golden","SDK public fields",err,false)
}

func otherOnlineControls() {
    cases:=[]struct{name string; mutate func(*batch,*controlAPI)}{
        {"missing-ack",func(b *batch,_ *controlAPI){delete(b.acked,b.plan.Inputs[0].ID)}},
        {"extra-ack",func(b *batch,_ *controlAPI){b.acked["extra"]=true}},
        {"missing-terminal",func(b *batch,_ *controlAPI){delete(b.done,key(queue(b.plan.Seed,"ordinary"),b.plan.Inputs[0].ID))}},
        {"extra-terminal",func(b *batch,_ *controlAPI){b.done["extra"]=true}},
        {"extra-retained",func(_ *batch,a *controlAPI){a.retained=[]*mq.PeekedMessage{{MessageID:"unexpected",State:mq.Deferred}}}},
    }
    for _,c:=range cases{
        b,a,_:=fixture(c.name)
        for _,m:=range b.plan.Inputs {b.done[key(m.Targets[0],m.ID)]=true}
        c.mutate(b,a)
        err:=b.reconcile(context.Background())
        if closeErr:=b.ledger.close();closeErr!=nil{panic(closeErr)}
        recordControl(c.name,"reconcile acknowledged/terminal/retained identity sets",err,true)
    }
    for _,c:=range []struct{name string;actual error;reject bool}{
        {"fencing-rejection",mq.ErrLockLost,false},
        {"fencing-success",nil,true},
        {"fencing-wrong-error",errors.New("unrelated transport error"),true},
    }{
        b,_,rows:=fixture(c.name)
        err:=b.expectError("expired-token",queue(b.plan.Seed,"ordinary"),rows[0],c.actual,mq.ErrLockLost)
        if closeErr:=b.ledger.close();closeErr!=nil{panic(closeErr)}
        recordControl(c.name,"actual expected-rejection oracle",err,c.reject)
    }
    for _,c:=range []struct{name string;mutate func(*mq.PeekedMessage);reject bool}{
        {"retained-valid",func(*mq.PeekedMessage){},false},
        {"retained-body-middle",func(p *mq.PeekedMessage){p.Body[len(p.Body)/2]^=1},true},
        {"retained-body-end",func(p *mq.PeekedMessage){p.Body[len(p.Body)-1]^=1},true},
        {"retained-metadata",func(p *mq.PeekedMessage){p.Subject+="wrong"},true},
        {"retained-sequence",func(p *mq.PeekedMessage){p.SequenceNumber++},true},
        {"retained-count",func(p *mq.PeekedMessage){p.DeliveryCount++},true},
        {"retained-state",func(p *mq.PeekedMessage){p.State=mq.Active},true},
        {"retained-reason",func(p *mq.PeekedMessage){p.DeadLetterReason="wrong"},true},
        {"retained-description",func(p *mq.PeekedMessage){p.DeadLetterDescription="wrong"},true},
        {"retained-expiry",func(p *mq.PeekedMessage){p.ExpiresAt=time.Now()},true},
        {"retained-enqueue",func(p *mq.PeekedMessage){p.EnqueuedAt=p.EnqueuedAt.Add(time.Second)},true},
        {"retained-lease",func(p *mq.PeekedMessage){p.LockedUntil=time.Now().Add(time.Second)},true},
    }{
        b,_,rows:=fixture(c.name)
        m:=rows[0]; q:=queue(b.plan.Seed,"ordinary")
        b.owners[key(q,m.MessageID)]=ownership{m.SequenceNumber,1,m.LockToken(),m.EnqueuedAt,m.LockedUntil}
        p:=&mq.PeekedMessage{SequenceNumber:m.SequenceNumber,MessageID:m.MessageID,Body:append([]byte(nil),m.Body...),
            State:mq.Deferred,GroupID:m.GroupID,Subject:m.Subject,CorrelationID:m.CorrelationID,
            ReplyTo:m.ReplyTo,ContentType:m.ContentType,Properties:m.Properties,DeliveryCount:1,EnqueuedAt:m.EnqueuedAt}
        c.mutate(p)
        err:=b.checkRetained(q,p,0,1,mq.Deferred,"","")
        if closeErr:=b.ledger.close();closeErr!=nil{panic(closeErr)}
        recordControl(c.name,"online retained full-content/state oracle",err,c.reject)
    }
    for _,c:=range []struct{name string;mutate func(*batch)}{
        {"online-count-contract",func(b *batch){b.deliveries++}},
        {"online-recipe-missing",func(b *batch){delete(b.hits,"renew")}},
        {"online-recipe-extra",func(b *batch){b.hits["unplanned"]=true}},
    }{
        b,_,_:=fixture(c.name)
        want:=expectedSummary(0,0)
        b.sent,b.deliveries,b.replays=want.LogicalSends,want.Deliveries,want.Replays
        for _,name:=range want.Recipes{b.hits[name]=true}
        c.mutate(b)
        // An invalid batch must fail before touching runner history or lane state.
        err:=(&runner{}).verified(b,time.Now())
        if closeErr:=b.ledger.close();closeErr!=nil{panic(closeErr)}
        recordControl(c.name,"verified independent recipe/counter contract",err,true)
    }
}

func syntheticHistory() (map[string]any,[]batchRecord,map[string]any) {
    seed:="0123456789abcdef0123456789abcdef"
    start:=time.UnixMilli(time.Now().Add(-time.Minute).UnixMilli())
    cfg:=config{Duration:time.Minute,MaxGap:120*time.Second}
    meta:=map[string]any{"seed":seed,"recipe_version":recipeVersion,"config":cfg,"started_at":start,
        "control_fixture":true,"purpose":"synthetic oracle controls; no workload or acceptance proof"}
    lanes:=make([]laneProgress,len(laneNames))
    for i,name:=range laneNames{lanes[i]=laneProgress{Name:name,Recipes:map[string]uint64{}}}
    var records []batchRecord
    for batch:=uint64(0);batch<3;batch++{
        for lane:=range laneNames{
            created:=start.Add(time.Duration(batch)*10*time.Second)
            p:=makePlan(seed,lane,batch,created.UnixMilli())
            data,_:=encoded(p)
            rec:=expectedSummary(lane,batch)
            rec.Version,rec.Seed,rec.Lane,rec.Batch=recipeVersion,seed,lane,batch
            rec.CreatedAt,rec.VerifiedAt=created.UnixMilli(),created.Add(time.Second).UnixMilli()
            rec.ExpectedHash,rec.AckHash,rec.ObservedHash=sumBytes(data),sumBytes([]byte("synthetic ack")),sumBytes([]byte("synthetic observation"))
            rec.AckRecords,rec.Observations=1,1
            records=append(records,rec)
            l:=&lanes[lane];l.Batches++;l.LogicalSends+=rec.LogicalSends;l.Deliveries+=rec.Deliveries;l.Replays+=rec.Replays
            l.LastVerified=time.UnixMilli(rec.VerifiedAt);l.LargestGap=10
            for _,r:=range rec.Recipes{l.Recipes[r]++}
        }
    }
    res:=map[string]any{"status":"SMOKE","production_ready":false,"elapsed_seconds":60,
        "requested_seconds":60,"validated_activity_seconds":60,"finished_at":start.Add(time.Minute),
        "verified_batches":len(records),"lanes":lanes}
    return meta,records,res
}

func auditControls() {
    cases:=[]struct{name string; mutate func(map[string]any,*[]batchRecord,map[string]any);reject bool}{
        {"history-valid-synthetic",func(map[string]any,*[]batchRecord,map[string]any){},false},
        {"history-payload-plan",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].ExpectedHash=sumBytes([]byte("wrong body manifest"))},true},
        {"history-ack-missing",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].AckRecords=0},true},
        {"history-observation-missing",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Observations=0},true},
        {"history-invalid-digest",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].ObservedHash="bad"},true},
        {"history-count",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Deliveries++},true},
        {"history-sends",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].LogicalSends++},true},
        {"history-replay-count",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Replays++},true},
        {"history-recipe-missing",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Recipes=(*r)[0].Recipes[1:]},true},
        {"history-recipe-extra",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Recipes=append((*r)[0].Recipes,"invented")},true},
        {"history-sequence",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Batch++},true},
        {"history-identity",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].Seed="fedcba9876543210fedcba9876543210"},true},
        {"history-missing",func(_ map[string]any,r *[]batchRecord,_ map[string]any){*r=(*r)[1:]},true},
        {"history-extra",func(_ map[string]any,r *[]batchRecord,_ map[string]any){*r=append(*r,(*r)[0])},true},
        {"history-negative-time",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].VerifiedAt=(*r)[0].CreatedAt-1},true},
        {"history-lane-stall",func(_ map[string]any,r *[]batchRecord,_ map[string]any){(*r)[0].VerifiedAt+=121000},true},
        {"result-false-pass",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["status"]="PASS";r["production_ready"]=true},true},
        {"result-false-production",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["production_ready"]=true},true},
        {"result-incomplete-duration",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["elapsed_seconds"]=59},true},
        {"result-requested-duration",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["requested_seconds"]=86400},true},
        {"result-missing-activity",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["validated_activity_seconds"]=0},true},
        {"result-clock-drift",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["max_clock_drift_seconds"]=6},true},
        {"result-heartbeat-gap",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["max_heartbeat_gap_seconds"]=11},true},
        {"result-lane-count",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["lanes"].([]laneProgress)[0].Batches++},true},
        {"result-recipe-hits",func(_ map[string]any,_ *[]batchRecord,r map[string]any){r["lanes"].([]laneProgress)[0].Recipes["renew"]++},true},
        {"metadata-short-duration",func(m map[string]any,_ *[]batchRecord,_ map[string]any){cfg:=m["config"].(config);cfg.Duration=time.Second;m["config"]=cfg},true},
    }
    for _,c:=range cases{
        meta,records,res:=syntheticHistory()
        c.mutate(meta,&records,res)
        // Re-sign every edited record: semantic negatives must not fail only on hashes.
        var chain string
        var history strings.Builder
        for i:=range records{
            records[i].Previous=chain
            records[i].Hash,_=recordHash(records[i]);chain=records[i].Hash
            data,_:=encoded(records[i]);history.Write(data);history.WriteByte('\n')
        }
        res["chain_sha256"]=chain
        dir:=filepath.Join(controlRoot,c.name)
        if err:=os.MkdirAll(filepath.Join(dir,"pending"),0700);err!=nil{panic(err)}
        if err:=atomicJSON(filepath.Join(dir,"metadata.json"),meta);err!=nil{panic(err)}
        if err:=atomicJSON(filepath.Join(dir,"result.json"),res);err!=nil{panic(err)}
        if err:=os.WriteFile(filepath.Join(dir,"batches.jsonl"),[]byte(history.String()),0600);err!=nil{panic(err)}
        recordControl(c.name,"offline audit reconstructed plan/recipes/duration (re-signed history)",audit(dir),c.reject)
    }
    // Structural corruption controls use the valid baseline, without re-signing.
    base:=filepath.Join(controlRoot,"history-valid-synthetic")
    for _,name:=range []string{"history-broken-chain","history-broken-hash","history-truncated-json","history-pending"}{
        dir:=filepath.Join(controlRoot,name);if err:=os.MkdirAll(filepath.Join(dir,"pending"),0700);err!=nil{panic(err)}
        for _,f:=range []string{"metadata.json","result.json","batches.jsonl"}{
            data,err:=os.ReadFile(filepath.Join(base,f));if err!=nil{panic(err)}
            if f=="batches.jsonl"{
                if name=="history-truncated-json"{data=data[:len(data)-3]}
                if name=="history-broken-hash" || name=="history-broken-chain"{
                    lines:=strings.Split(strings.TrimSuffix(string(data),"\n"),"\n")
                    var rec batchRecord
                    if err:=json.Unmarshal([]byte(lines[1]),&rec);err!=nil{panic(err)}
                    if name=="history-broken-chain"{rec.Previous=sumBytes([]byte("wrong predecessor"))}else{rec.Hash=sumBytes([]byte("wrong hash"))}
                    out,_:=encoded(rec);lines[1]=string(out);data=[]byte(strings.Join(lines,"\n")+"\n")
                }
            }
            if err:=os.WriteFile(filepath.Join(dir,f),data,0600);err!=nil{panic(err)}
        }
        if name=="history-pending"{if err:=os.WriteFile(filepath.Join(dir,"pending","unverified"),[]byte("pending"),0600);err!=nil{panic(err)}}
        recordControl(name,"offline audit structural evidence",audit(dir),true)
    }
}

func main() {
    controlRoot=os.Args[1]
    if err:=os.MkdirAll(controlRoot,0700);err!=nil{panic(err)}
    receiveControls()
    otherOnlineControls()
    auditControls()
    for _,c:=range []struct{name string;requested,elapsed time.Duration;status string;reject bool}{
        {"classification-short",time.Minute,time.Minute,"SMOKE",false},
        {"classification-under-24h",24*time.Hour,24*time.Hour-time.Nanosecond,"FAIL",true},
        {"classification-24h",24*time.Hour,24*time.Hour,"PASS",false},
        {"classification-short-request-long-elapsed",time.Minute,24*time.Hour,"SMOKE",false},
    }{
        status,production,err:=classify(c.requested,c.elapsed)
        if status!=c.status || production!=(c.status=="PASS"){panic("classification status mismatch")}
        recordControl(c.name,"duration classifier boundary",err,c.reject)
    }
    result:=map[string]any{"status":"PASS","production_acceptance":false,"control_count":len(controls),
        "controls":controls,"note":"Synthetic controls call the actual online oracles; synthetic history is not workload evidence."}
    if err:=atomicJSON(filepath.Join(controlRoot,"controls.json"),result);err!=nil{panic(err)}
    fmt.Printf("oracle controls passed: %d\n",len(controls))
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    repo = Path(__file__).resolve().parents[2]
    source = repo / "test/production/soak"
    hashes = {}
    with tempfile.TemporaryDirectory(prefix="mqlite-soak-controls-") as temp:
        work = Path(temp)
        for name in ("main.go", "ledger.go", "lanes.go"):
            data = (source / name).read_bytes()
            hashes[name] = hashlib.sha256(data).hexdigest()
            if name == "main.go":
                if data.count(b"func main() {") != 1:
                    raise RuntimeError("runner entry point changed; review control source wiring")
                data = data.replace(b"func main() {", b"func originalSoakMain() {")
            (work / name).write_bytes(data)
        (work / "controls.go").write_text(CONTROL)
        command = ["go", "run", *(str(work / n) for n in ("main.go", "ledger.go", "lanes.go", "controls.go")), str(output)]
        env = dict(os.environ)
        completed = subprocess.run(command, cwd=repo, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        (output / "run.log").write_text(completed.stdout)
        print(completed.stdout, end="")
        (output / "source.json").write_text(json.dumps({
            "files_sha256": hashes,
            "controls_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "goflags": env.get("GOFLAGS", ""),
            "toolchain": env.get("GOTOOLCHAIN", "auto"),
            "exit_code": completed.returncode,
        }, indent=2) + "\n")
        if completed.returncode:
            raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
