import asyncio
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple
import threading
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws, order_ws
import time as time_module

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SENSEXOptionsBot:
    def __init__(self, app_id: str, access_token: str, lots: int = 1):
        self.app_id = app_id
        self.access_token = access_token
        self.auth_token = f"{app_id}:{access_token}"
        self.lots = lots
        
        # Initialize Fyers API
        self.fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)
        
        # Trading parameters
        self.max_sl_per_day = 3
        self.sl_count_today = 0
        self.hedge_premium_target = 5.0
        self.lot_size = 20  # SENSEX options lot size
        
        # Market hours
        self.market_start = time(9, 15)
        self.market_end = time(15, 0)
        
        # Data storage
        self.candle_data = {
            'CE': pd.DataFrame(),
            'PE': pd.DataFrame()
        }
        self.one_min_data = {
            'CE': pd.DataFrame(),
            'PE': pd.DataFrame()
        }
        self.positions = {}
        self.active_orders = {}
        
        # ATM strikes and symbols
        self.atm_ce_symbol = None
        self.atm_pe_symbol = None
        self.current_spot = None
        self.current_atm_strike = None
        
        # WebSocket connections
        self.data_socket = None
        self.order_socket = None
        
        # Thread lock for data updates
        self.data_lock = threading.Lock()
        
    def is_market_hours(self) -> bool:
        """Check if current time is within market hours"""
        now = datetime.now().time()
        return self.market_start <= now <= self.market_end
    
    def is_doji_or_equal(self, candle: pd.Series) -> bool:
        """Check if candle is doji or equal open/close"""
        return abs(candle['close'] - candle['open']) <= 0.01
    
    def is_red_candle(self, candle: pd.Series) -> bool:
        """Check if candle is red (including doji and equal candles)"""
        return candle['close'] <= candle['open']
    
    def is_green_candle(self, candle: pd.Series) -> bool:
        """Check if candle is green"""
        return candle['close'] > candle['open']
    
    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average"""
        return data.ewm(span=period).mean()
    
    def calculate_vwma(self, close: pd.Series, volume: pd.Series, period: int) -> pd.Series:
        """Calculate Volume Weighted Moving Average"""
        vwma = (close * volume).rolling(window=period).sum() / volume.rolling(window=period).sum()
        return vwma
    
    def get_atm_strikes(self, spot_price: float) -> Tuple[str, str]:
        """Get ATM CE and PE symbols based on spot price"""
        # Round to nearest 100 for SENSEX
        atm_strike = round(spot_price / 100) * 100
        
        # Get current expiry (you'll need to implement expiry logic)
        expiry = self.get_current_expiry()
        
        # Format: NSE:SENSEX24DECCE75000 (example format - adjust as per Fyers)
        ce_symbol = f"BSE:SENSEX{expiry}CE{int(atm_strike)}"
        pe_symbol = f"BSE:SENSEX{expiry}PE{int(atm_strike)}"
        
        return ce_symbol, pe_symbol
    
    def get_nearest_weekly_expiry(self) -> str:
        """Get nearest weekly expiry in DDMMMYY format"""
        try:
            # Get current date
            now = datetime.now()
            
            # Find next Thursday (weekly expiry day)
            days_until_thursday = (3 - now.weekday()) % 7  # Thursday is weekday 3
            if days_until_thursday == 0 and now.hour >= 15:  # If it's Thursday after market close
                days_until_thursday = 7  # Next Thursday
            
            next_thursday = now + timedelta(days=days_until_thursday)
            
            # Format as DDMMMYY (e.g., 28DEC23)
            expiry_str = next_thursday.strftime("%d%b%y").upper()
            return expiry_str
            
        except Exception as e:
            logger.error(f"Error calculating expiry: {e}")
            # Fallback to current month expiry
            return datetime.now().strftime("%d%b%y").upper()
    
    def get_atm_strikes(self, spot_price: float) -> Tuple[str, str, float]:
        """Get ATM CE and PE symbols based on spot price"""
        # Round to nearest 100 for SENSEX
        atm_strike = round(spot_price / 100) * 100
        
        # Get nearest weekly expiry
        expiry = self.get_nearest_weekly_expiry()
        
        # Format: BSE:SENSEX2590480200CE (example format you provided)
        # Extract date part for symbol (DDMMMYY format to YYMMDD or similar)
        expiry_date = datetime.strptime(expiry, "%d%b%y")
        date_part = int(f"{expiry_date.strftime('%y')}{expiry_date.month}{expiry_date.day:02d}")
        
        ce_symbol = f"BSE:SENSEX{date_part}{int(atm_strike)}CE"
        pe_symbol = f"BSE:SENSEX{date_part}{int(atm_strike)}PE"
        
        return ce_symbol, pe_symbol, atm_strike
    
    def check_pattern(self, df: pd.DataFrame) -> bool:
        """Check for the 5-candle pattern (R-G-R-G-R with breakdown)"""
        if len(df) < 5:
            return False
        
        # Get last 5 candles
        last_5 = df.tail(5)
        candles = last_5.to_dict('records')
        
        # Pattern: Red-Green-Red-Green-Red
        pattern_check = [
            self.is_red_candle(pd.Series(candles[0])),    # 1st: Red
            self.is_green_candle(pd.Series(candles[1])),  # 2nd: Green
            self.is_red_candle(pd.Series(candles[2])),    # 3rd: Red
            self.is_green_candle(pd.Series(candles[3])),  # 4th: Green
            self.is_red_candle(pd.Series(candles[4]))     # 5th: Red (breakdown)
        ]
        
        if not all(pattern_check):
            return False
        
        # Check if 5th candle closes below the lowest of previous 4 candles
        prev_4_lows = [candles[i]['low'] for i in range(4)]
        breakdown_close = candles[4]['close']
        
        if breakdown_close >= min(prev_4_lows):
            return False
        
        # Check EMA5 < VWMA20 condition on breakdown candle
        if len(df) >= 20:
            ema5 = self.calculate_ema(df['close'], 5).iloc[-1]
            vwma20 = self.calculate_vwma(df['close'], df['volume'], 20).iloc[-1]
            
            if ema5 >= vwma20:
                return False
        
        return True
    
    def check_exit_condition(self, df: pd.DataFrame, position_type: str) -> bool:
        """Check exit conditions"""
        if len(df) < 20:
            return False
        
        last_candle = df.iloc[-1]
        
        # Exit if green candle closes above both EMA5 and VWMA20
        if self.is_green_candle(last_candle):
            ema5 = self.calculate_ema(df['close'], 5).iloc[-1]
            vwma20 = self.calculate_vwma(df['close'], df['volume'], 20).iloc[-1]
            
            if last_candle['close'] > max(ema5, vwma20):
                return True
        
        return False
    
    def place_order(self, symbol: str, side: str, quantity: int, order_type: str = "LIMIT", price: float = None) -> Optional[str]:
        """Place order using Fyers API"""
        try:
            order_data = {
                "symbol": symbol,
                "qty": quantity,
                "type": 2 if order_type == "LIMIT" else 1,  # 1: Market, 2: Limit
                "side": 1 if side == "BUY" else -1,  # 1: Buy, -1: Sell
                "productType": "INTRADAY",
                "limitPrice": price if order_type == "LIMIT" else 0,
                "stopPrice": 0,
                "validity": "DAY",
                "offlineOrder": False
            }
            
            response = self.fyers.place_order(order_data)
            
            if response['s'] == 'ok':
                order_id = response['id']
                logger.info(f"Order placed successfully: {order_id} - {side} {quantity} {symbol} @ {price}")
                return order_id
            else:
                logger.error(f"Order placement failed: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return None
    
    def place_stop_loss(self, symbol: str, quantity: int, sl_price: float) -> Optional[str]:
        """Place stop loss order"""
        try:
            # Use market order for SL
            order_data = {
                "symbol": symbol,
                "qty": quantity,
                "type": 3,  # Stop Loss Market
                "side": 1,  # Buy to cover short
                "productType": "INTRADAY",
                "limitPrice": 0,
                "stopPrice": sl_price,
                "validity": "DAY",
                "offlineOrder": False
            }
            
            response = self.fyers.place_order(order_data)
            
            if response['s'] == 'ok':
                order_id = response['id']
                logger.info(f"Stop Loss placed: {order_id} - BUY {quantity} {symbol} @ {sl_price}")
                return order_id
            else:
                logger.error(f"SL order placement failed: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Error placing SL: {e}")
            return None
    
    def execute_trade(self, symbol: str, option_type: str, breakdown_candle: pd.Series):
        """Execute the main trade with hedge"""
        if self.sl_count_today >= self.max_sl_per_day:
            logger.info("Maximum SL count reached for today. Skipping trade.")
            return
        
        try:
            # Get current price for limit order
            quote = self.fyers.quotes({"symbols": symbol})
            if quote['s'] != 'ok':
                logger.error("Failed to get quote for entry")
                return
            
            current_price = quote['d'][0]['v']['lp']
            
            # Calculate position size (lots * lot_size)
            quantity = self.lots * self.lot_size
            
            # Place primary sell order
            primary_order_id = self.place_order(
                symbol=symbol,
                side="SELL",
                quantity=quantity,
                order_type="LIMIT",
                price=current_price
            )
            
            if not primary_order_id:
                return
            
            # Calculate stop loss (high of breakdown candle + 2 points)
            sl_price = breakdown_candle['high'] + 2.0
            
            # Place stop loss
            sl_order_id = self.place_stop_loss(symbol, quantity, sl_price)
            
            # Find and buy hedge
            atm_strike = float(symbol.split('CE')[-1] if 'CE' in symbol else symbol.split('PE')[-1])
            hedge_symbol = self.get_hedge_option(symbol, option_type, atm_strike)
            
            hedge_order_id = None
            if hedge_symbol:
                hedge_quote = self.fyers.quotes({"symbols": hedge_symbol})
                if hedge_quote['s'] == 'ok':
                    hedge_price = hedge_quote['d'][0]['v']['lp']
                    hedge_order_id = self.place_order(
                        symbol=hedge_symbol,
                        side="BUY",
                        quantity=quantity,
                        order_type="LIMIT",
                        price=hedge_price
                    )
            
            # Store position details
            position_key = f"{symbol}_{datetime.now().strftime('%H%M%S')}"
            self.positions[position_key] = {
                'symbol': symbol,
                'option_type': option_type,
                'quantity': quantity,
                'entry_price': current_price,
                'sl_price': sl_price,
                'primary_order_id': primary_order_id,
                'sl_order_id': sl_order_id,
                'hedge_symbol': hedge_symbol,
                'hedge_order_id': hedge_order_id,
                'entry_time': datetime.now(),
                'status': 'ACTIVE'
            }
            
            logger.info(f"Trade executed: {position_key}")
            
        except Exception as e:
            logger.error(f"Error executing trade: {e}")
    
    def monitor_positions(self):
        """Monitor active positions for exit conditions"""
        for pos_key, position in list(self.positions.items()):
            if position['status'] != 'ACTIVE':
                continue
            
            try:
                symbol = position['symbol']
                option_type = position['option_type']
                
                # Get current candle data
                df = self.candle_data[option_type]
                
                if len(df) == 0:
                    continue
                
                # Check exit condition
                if self.check_exit_condition(df, option_type):
                    self.close_position(pos_key, "EXIT_CONDITION")
                
            except Exception as e:
                logger.error(f"Error monitoring position {pos_key}: {e}")
    
    def close_position(self, position_key: str, reason: str):
        """Close position and hedge"""
        try:
            position = self.positions[position_key]
            
            # Cancel SL order if active
            if position['sl_order_id']:
                self.fyers.cancel_order({"id": position['sl_order_id']})
            
            # Close primary position (buy to cover short)
            close_order_id = self.place_order(
                symbol=position['symbol'],
                side="BUY",
                quantity=position['quantity'],
                order_type="MARKET"
            )
            
            # Close hedge position
            if position['hedge_symbol'] and position['hedge_order_id']:
                hedge_close_order_id = self.place_order(
                    symbol=position['hedge_symbol'],
                    side="SELL",
                    quantity=position['quantity'],
                    order_type="MARKET"
                )
            
            # Update position status
            position['status'] = 'CLOSED'
            position['close_reason'] = reason
            position['close_time'] = datetime.now()
            
            logger.info(f"Position closed: {position_key} - Reason: {reason}")
            
            if reason == "STOP_LOSS":
                self.sl_count_today += 1
                
        except Exception as e:
            logger.error(f"Error closing position {position_key}: {e}")
    
    def update_candle_data(self, option_type: str, candle_data: Dict):
        """Update candle data from websocket"""
        try:
            with self.data_lock:
                # Convert to DataFrame row
                new_candle = pd.DataFrame([{
                    'timestamp': candle_data.get('timestamp', datetime.now()),
                    'open': candle_data.get('open', 0),
                    'high': candle_data.get('high', 0),
                    'low': candle_data.get('low', 0),
                    'close': candle_data.get('close', 0),
                    'volume': candle_data.get('volume', 0)
                }])
                
                # Store 1-minute data
                if len(self.one_min_data[option_type]) == 0:
                    self.one_min_data[option_type] = new_candle
                else:
                    self.one_min_data[option_type] = pd.concat([
                        self.one_min_data[option_type], 
                        new_candle
                    ], ignore_index=True)
                
                # Keep only last 200 1-minute candles (for building 3-min candles)
                if len(self.one_min_data[option_type]) > 200:
                    self.one_min_data[option_type] = self.one_min_data[option_type].tail(200).reset_index(drop=True)
                
                # Build 3-minute candles
                self.build_3min_candles(option_type)
                
                # Check for pattern after each 3-minute candle update
                if len(self.candle_data[option_type]) >= 5:
                    if self.check_pattern(self.candle_data[option_type]):
                        breakdown_candle = self.candle_data[option_type].iloc[-1]
                        current_symbol = self.atm_ce_symbol if option_type == 'CE' else self.atm_pe_symbol
                        logger.info(f"Pattern detected on {option_type} - {current_symbol}")
                        self.execute_trade(current_symbol, option_type, breakdown_candle)
            
        except Exception as e:
            logger.error(f"Error updating candle data: {e}")
    
    def get_spot_price(self) -> float:
        """Get current SENSEX spot price"""
        try:
            # Get SENSEX quote - adjust symbol as needed for BSE SENSEX
            response = self.fyers.quotes({"symbols": "BSE:SENSEX-INDEX"})
            if response['s'] == 'ok':
                return response['d'][0]['v']['lp']
            return None
        except Exception as e:
            logger.error(f"Error getting spot price: {e}")
            return None
    
    def initialize_symbols(self):
        """Initialize ATM symbols based on current spot price"""
        spot_price = self.get_spot_price()
        if spot_price:
            self.current_spot = spot_price
            self.atm_ce_symbol, self.atm_pe_symbol, self.current_atm_strike = self.get_atm_strikes(spot_price)
            logger.info(f"Spot: {spot_price}, ATM Strike: {self.current_atm_strike}")
            logger.info(f"CE: {self.atm_ce_symbol}, PE: {self.atm_pe_symbol}")
    
    # WebSocket callback functions
    def on_market_data(self, message):
        """Handle incoming market data from WebSocket"""
        try:
            if message.get('s') != 'ok':
                return
            
            for data in message.get('d', []):
                symbol = data.get('symbol', '')
                
                # Check if this is our ATM option
                option_type = None
                if symbol == self.atm_ce_symbol:
                    option_type = 'CE'
                elif symbol == self.atm_pe_symbol:
                    option_type = 'PE'
                
                if not option_type:
                    continue
                
                # Process candle data (assuming 1-minute candles from WebSocket)
                candle_info = {
                    'timestamp': datetime.now(),
                    'open': data.get('o', 0),
                    'high': data.get('h', 0),
                    'low': data.get('l', 0),
                    'close': data.get('ltp', 0),
                    'volume': data.get('v', 0)
                }
                
                self.update_candle_data(option_type, candle_info)
                
        except Exception as e:
            logger.error(f"Error processing market data: {e}")
    
    def on_order_update(self, message):
        """Handle order updates from WebSocket"""
        try:
            if message.get('s') != 'ok':
                return
            
            order_data = message.get('orders', {})
            order_id = order_data.get('id', '')
            status = order_data.get('status', 0)
            
            # Check if this is a stop-loss order that got executed
            for pos_key, position in self.positions.items():
                if position.get('sl_order_id') == order_id and status == 90:  # 90 = Executed
                    logger.info(f"Stop Loss triggered for position: {pos_key}")
                    self.close_position(pos_key, "STOP_LOSS")
                    break
            
            logger.info(f"Order update: {order_id} - Status: {status}")
            
        except Exception as e:
            logger.error(f"Error processing order update: {e}")
    
    def setup_websockets(self):
        """Setup WebSocket connections for market data and orders"""
        try:
            # Setup market data WebSocket
            self.data_socket = data_ws.FyersDataSocket(
                access_token=self.auth_token,
                log_path="",
                litemode=False,
                write_to_file=False,
                on_connect=self.on_data_connect,
                on_close=self.on_data_close,
                on_error=self.on_data_error,
                on_message=self.on_market_data
            )
            
            # Setup order WebSocket
            self.order_socket = order_ws.FyersOrderSocket(
                access_token=self.auth_token,
                write_to_file=False,
                log_path="",
                on_connect=self.on_order_connect,
                on_close=self.on_order_close,
                on_error=self.on_order_error,
                on_orders=self.on_order_update
            )
            
            logger.info("WebSocket connections initialized")
            
        except Exception as e:
            logger.error(f"Error setting up WebSockets: {e}")
    
    def on_data_connect(self):
        """Callback for market data WebSocket connection"""
        try:
            logger.info("Market data WebSocket connected")
            if self.atm_ce_symbol and self.atm_pe_symbol:
                # Subscribe to ATM options data
                symbols = [self.atm_ce_symbol, self.atm_pe_symbol]
                self.data_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
                self.data_socket.keep_running()
        except Exception as e:
            logger.error(f"Error in data connect callback: {e}")
    
    def on_order_connect(self):
        """Callback for order WebSocket connection"""
        try:
            logger.info("Order WebSocket connected")
            self.order_socket.subscribe(data_type="OnOrders")
            self.order_socket.keep_running()
        except Exception as e:
            logger.error(f"Error in order connect callback: {e}")
    
    def on_data_close(self, message):
        """Callback for market data WebSocket close"""
        logger.info(f"Market data WebSocket closed: {message}")
    
    def on_order_close(self, message):
        """Callback for order WebSocket close"""
        logger.info(f"Order WebSocket closed: {message}")
    
    def on_data_error(self, message):
        """Callback for market data WebSocket error"""
        logger.error(f"Market data WebSocket error: {message}")
    
    def on_order_error(self, message):
        """Callback for order WebSocket error"""
        logger.error(f"Order WebSocket error: {message}")
    
    def start_websockets(self):
        """Start WebSocket connections in separate threads"""
        try:
            if self.data_socket:
                data_thread = threading.Thread(target=self.data_socket.connect, daemon=True)
                data_thread.start()
            
            if self.order_socket:
                order_thread = threading.Thread(target=self.order_socket.connect, daemon=True)
                order_thread.start()
                
            logger.info("WebSocket threads started")
            
        except Exception as e:
            logger.error(f"Error starting WebSocket threads: {e}")
    
    async def run(self):
        """Main trading loop"""
        logger.info("Starting SENSEX Options Trading Bot")
        
        # Initialize symbols
        self.initialize_symbols()
        
        # Setup and start WebSocket connections
        self.setup_websockets()
        await asyncio.sleep(2)  # Give time for WebSocket setup
        self.start_websockets()
        
        # Reset daily counters
        self.sl_count_today = 0
        
        logger.info("Bot is now running and monitoring patterns...")
        
        # Main loop
        while True:
            try:
                if not self.is_market_hours():
                    logger.info("Outside market hours. Sleeping...")
                    await asyncio.sleep(300)  # 5 minutes
                    continue
                
                # Monitor existing positions
                self.monitor_positions()
                
                # Update ATM symbols every 15 minutes or if spot moves significantly
                current_time = datetime.now()
                if (current_time.minute % 15 == 0 or 
                    (self.current_spot and self.get_spot_price() and 
                     abs(self.get_spot_price() - self.current_spot) > 200)):
                    
                    old_ce = self.atm_ce_symbol
                    old_pe = self.atm_pe_symbol
                    
                    self.initialize_symbols()
                    
                    # If symbols changed, update WebSocket subscriptions
                    if (old_ce != self.atm_ce_symbol or old_pe != self.atm_pe_symbol):
                        logger.info("ATM symbols updated, resubscribing to data...")
                        if self.data_socket:
                            symbols = [self.atm_ce_symbol, self.atm_pe_symbol]
                            self.data_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(60)

# WebSocket handler for real-time data (legacy - now handled in class)
def on_message(message):
    """Handle incoming websocket messages - Legacy function"""
    pass

if __name__ == "__main__":
    # Configuration
    APP_ID = ""
    ACCESS_TOKEN = ""
    print("=== Attempting Official Library Approach ===")
    LOTS = 1  # Number of lots to trade
    
    # Create and run bot
    bot = SENSEXOptionsBot(APP_ID, ACCESS_TOKEN, LOTS)
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot shutdown complete")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        logger.info("Bot shutdown complete")
