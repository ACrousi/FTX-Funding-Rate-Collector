# FTX Funding Rate

Collect all FTX perprual future pair's funding_rate and their premium rate with spot price

## Column Description
`coin` : ftx perprual future pair  
`last_rate` : funding rate at last hour  
`now_apy` : calculate by last_rate  
`avg_apy` : calculate by last 500 hours funding rate  
`predict_rate` : predict next hour funding rate  
`perp_vol` : last 24 perptual future trading volume  
`exfuture_prm` : premium rate between quarterly and perpetual future  
`exfuture_vol` : last 24 quarterly future trading volume  
`spot_prm` : premium rate between spot and perpetual future  
`spot_vol` : last 24 spot trading volume  
`eligable_to_hedge` : if this pair is available to hedge price to earn funding payment  

