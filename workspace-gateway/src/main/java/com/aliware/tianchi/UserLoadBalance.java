package com.aliware.tianchi;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import com.aliware.tianchi.comm.ServerLoadInfo;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance{
    
    
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
    	return selectInvokerIndex3(invokers);
//    	return selectInvokerIndex2(invokers);
//    	return selectInvokerIndex3(invokers);
    }
    

    private <T> Invoker<T> selectInvokerIndex1(List<Invoker<T>> invokers) {
    	
    	int size = invokers.size();
        int totalWeight = 0;
        List<Integer> hasPermitArr = new ArrayList<>();
        List<Integer> weightArr = new ArrayList<>();
        
        // 遍历更新每个invoker的可用权重
        for (int index=0; index<size; index++) {
        	
            Invoker<T> invoker = invokers.get(index);
            ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
            AtomicInteger limiter = UserLoadBalanceService.getAtomicInteger(invoker);
            
            if (serverLoadInfo != null) {
                int permits = limiter.get();
                int weight = serverLoadInfo.getWeight();
                if (permits > 0 ) {
                    // 根据耗时重新计算权重(基本权重*(1秒/单个请求耗时))
                    int clientTimeAvgSpendCurr = serverLoadInfo.getAvgSpendTime();
                    if (clientTimeAvgSpendCurr == 0) {
                        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                    }
                    
                    weight = weight * (500 / clientTimeAvgSpendCurr);
                    hasPermitArr.add(index);
                    weightArr.add(weight);
                    totalWeight = totalWeight+weight;
                }
            }
        }
        
        // 服务都被打满了,根据耗时选一个时间较短的服务端
        if (hasPermitArr.isEmpty()) {
            for (int index=0; index<size; index++) {
                ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
                if(serverLoadInfo != null){
                    int weight = serverLoadInfo.getWeight();
                    //根据耗时重新计算权重(基本权重*(1秒/单个请求耗时))
                    int clientTimeAvgSpendCurr = serverLoadInfo.getAvgSpendTime();
                    if(clientTimeAvgSpendCurr == 0){
                        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                    }
                    weight = weight*(500/clientTimeAvgSpendCurr);
                    hasPermitArr.add(index);
                    weightArr.add(weight);
                    totalWeight = totalWeight+weight;
                }
            }
        }
        
        // 根据服务端配置和平均耗时计算权重
        int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
        int average = totalWeight / 2;
        for (int i=0; i<hasPermitArr.size(); i++) {
            int index = hasPermitArr.get(i);
            int currentWeight = weightArr.get(i);
            
            if (offsetWeight > average && average < currentWeight) {
            	return invokers.get(index);
            }
            
            if (offsetWeight < average && offsetWeight < currentWeight) {
                return invokers.get(index);
            } 
            
        }
        
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
    
    private <T> Invoker<T> selectInvokerIndex2(List<Invoker<T>> invokers) {
    	
    	int size = invokers.size();
    	int totalWeight = 0;
    	List<Integer> hasPermitArr = new ArrayList<>();
    	List<Integer> weightArr = new ArrayList<>();
    	
    	// 首先获取invoker对应的服务端耗时最大的索引
    	for (int index=0; index<size; index++) {
    		
    		Invoker<T> invoker = invokers.get(index);
    		ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
    		AtomicInteger limiter = UserLoadBalanceService.getAtomicInteger(invoker);
    		
    		if (serverLoadInfo != null) {
    			int permits = limiter.get();
    			int weight = serverLoadInfo.getWeight();
    			if (permits > 0 ) {
    				// 根据耗时重新计算权重(基本权重*(1秒/单个请求耗时))
    				int clientTimeAvgSpendCurr = serverLoadInfo.getAvgSpendTime();
    				if (clientTimeAvgSpendCurr == 0) {
    					continue;
    				}
    				
    				weight = weight * (500 / clientTimeAvgSpendCurr);
    				hasPermitArr.add(index);
    				weightArr.add(weight);
    				totalWeight = totalWeight+weight;
    			}
    		}
    	}
    	
    	// 服务都被打满了,根据耗时选一个时间较短的服务端
    	if (hasPermitArr.isEmpty()) {
    		for (int index=0; index<size; index++) {
    			ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
    			if (serverLoadInfo != null) {
    				int weight = serverLoadInfo.getWeight();
    				//根据耗时重新计算权重(基本权重*(1秒/单个请求耗时))
    				int clientTimeAvgSpendCurr = serverLoadInfo.getAvgSpendTime();
    				if (clientTimeAvgSpendCurr == 0) {
    					continue;
    				}
    				weight = weight*(500/clientTimeAvgSpendCurr);
    				hasPermitArr.add(index);
    				weightArr.add(weight);
    				totalWeight = totalWeight+weight;
    			}
    		}
    	}
    	
    	// 根据服务端配置和平均耗时计算权重
    	int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
    	int average = totalWeight / 2;
    	for (int i=0; i<hasPermitArr.size(); i++) {
    		int index = hasPermitArr.get(i);
    		int currentWeight = weightArr.get(i);
    		
    		if (offsetWeight > average && average < currentWeight) {
    			return invokers.get(index);
    		}
    		
    		if (offsetWeight < average && offsetWeight < currentWeight) {
    			return invokers.get(index);
    		} 
    		
    	}
    	
    	return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
    
    private <T> Invoker<T> selectInvokerIndex3(List<Invoker<T>> invokers) {
    	
    	int totalWeight = 0;
    	int size = invokers.size();
        PriorityQueue<Threshold> hasPermitQueue = new PriorityQueue<Threshold>();
    	
    	// 首先获取invoker对应的服务端耗时最大的索引
    	for (int index=0; index < size; index++) {
    		
    		Invoker<T> invoker = invokers.get(index);
    		ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
    		AtomicInteger limiter = UserLoadBalanceService.getAtomicInteger(invoker);
    		
    		if (serverLoadInfo != null) {
    			int permits = limiter.get();
    			int weight = serverLoadInfo.getWeight();
    			if (permits > 0 ) {
    				// 根据耗时重新计算权重(基本权重*(1秒/单个请求耗时))
    				int clientTimeAvgSpendCurr = serverLoadInfo.getAvgSpendTime();
    				if (clientTimeAvgSpendCurr == 0) {
    					continue;
    				}
    				
    				weight = weight * (500 / clientTimeAvgSpendCurr);
    				long activeCount = serverLoadInfo.getActiveCount().get();
    				int efficValue = serverLoadInfo.getProviderThread() * 1000;
    				if (activeCount > 0) {
    					efficValue = new BigDecimal(serverLoadInfo.getProviderThread()).multiply(new BigDecimal(1000)).divide(
        						new BigDecimal(activeCount), RoundingMode.HALF_UP).intValue();
    				}
    				hasPermitQueue.add(new Threshold(index, weight, efficValue));
    				totalWeight = totalWeight + weight;
    			}
    		}
    	}
    	
    	// 服务都被打满了,根据耗时选一个时间较短的服务端
    	if (hasPermitQueue.isEmpty()) {
    		for (int index=0; index < size; index++) {
    			ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
    			if (serverLoadInfo != null) {
    				int weight = serverLoadInfo.getWeight();
    				//根据耗时重新计算权重(基本权重*(1秒/单个请求耗时))
    				int clientTimeAvgSpendCurr = serverLoadInfo.getAvgSpendTime();
    				if(clientTimeAvgSpendCurr == 0){
    					continue;
    				}
    				
    				weight = weight * (500 / clientTimeAvgSpendCurr);
    				long reqCount = serverLoadInfo.getReqCount().get();
    				int efficValue = serverLoadInfo.getProviderThread() * 1000;
    				if (reqCount > 0) {
    					efficValue = new BigDecimal(serverLoadInfo.getProviderThread()).multiply(new BigDecimal(1000)).divide(
        						new BigDecimal(reqCount), RoundingMode.HALF_UP).intValue();
    				}
    				hasPermitQueue.add(new Threshold(index, weight, efficValue));
    				totalWeight = totalWeight + weight;
    			}
    		}
    	}
    	
    	// 根据服务端配置和平均耗时计算权重
    	int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
    	int average = totalWeight / 2;
    	for (int i=0; i < hasPermitQueue.size(); i++) {
    		
    		Threshold hasPermit = hasPermitQueue.poll();
    		if (offsetWeight > average && average < hasPermit.weight) {
    			return invokers.get(hasPermit.index);
    		}
    		
    		if (offsetWeight < average && offsetWeight < hasPermit.weight) {
    			return invokers.get(hasPermit.index);
    		} 
    		
    	}
    	
    	return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
    

	private class Threshold implements Comparable<Threshold> {
		
	    int index, weight, effic;
	    
	    @Override
	    public int compareTo(Threshold o) {
	    	
	    	if (effic > o.effic) {
	    		return 1;
	    	} else if (effic == o.effic) {
	    		return o.weight - weight;
	    	}
	    	
	        return -1;
	    }
	    
	    public Threshold(int index, int weight, int effic){
	    	this.index = index;
	        this.weight = weight;
	        this.effic = effic;
	    }
	    
	}

    
}
