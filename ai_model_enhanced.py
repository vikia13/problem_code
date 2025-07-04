import logging
import numpy as np
import pandas as pd
import time
import json
import os
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


class EnhancedAIModel:
    """Enhanced AI model with multiple sub-models and improved database handling"""

    # In ai_model_enhanced.py
    def __init__(self, db_manager):
        try:
            self.db_manager = db_manager
            logger.info("Enhanced AI model initialized with multi-model support")
        except Exception as e:
            logger.error(f"Database manager not provided to EnhancedAIModel: {e}")

    def _init_models(self):
        """Initialize the AI models"""
        self.models = {
            'trend_reversal': {
                'accuracy': 0.65,
                'last_trained': 0
            },
            'momentum': {
                'accuracy': 0.68,
                'last_trained': 0
            },
            'breakout': {
                'accuracy': 0.72,
                'last_trained': 0
            }
        }

        # Load model metadata from database
        self._load_model_metadata()

    def _load_model_metadata(self):
        """Load model metadata from database"""
        if not self.db_manager:
            return

        try:
            result = self.db_manager.execute_query(
                'ai_model',
                'SELECT model_name, last_trained, accuracy, parameters FROM model_metadata',
                fetch='all'
            )

            if not result:
                return

            for row in result:
                model_name = row[0]
                if model_name in self.models:
                    self.models[model_name]['last_trained'] = row[1]
                    self.models[model_name]['accuracy'] = row[2]
                    if row[3]:
                        try:
                            self.models[model_name]['parameters'] = json.loads(row[3])
                        except json.JSONDecodeError:
                            pass
        except Exception as e:
            logger.error(f"Error loading model metadata: {e}")

    def predict(self, symbol, timeframe, indicators):
        """Make a prediction using the best suitable model"""
        if not indicators:
            return None

        try:
            # Select the best model for prediction
            best_model_name = self._select_best_model(symbol, timeframe, indicators)

            if not best_model_name:
                return None

            # Get latest price
            latest_price = self._extract_latest_price(indicators)

            if not latest_price:
                return None

            # Make prediction using the selected model
            if best_model_name == 'trend_reversal':
                prediction = self._predict_trend_reversal(symbol, timeframe, indicators, latest_price)
            elif best_model_name == 'momentum':
                prediction = self._predict_momentum(symbol, timeframe, indicators, latest_price)
            elif best_model_name == 'breakout':
                prediction = self._predict_breakout(symbol, timeframe, indicators, latest_price)
            else:
                return None

            # Store prediction
            self._store_prediction(best_model_name, symbol, timeframe, prediction)

            return prediction
        except Exception as e:
            logger.error(f"Error making prediction for {symbol} {timeframe}: {e}")
            return None

    def _select_best_model(self, symbol, timeframe, indicators):
        """Select the best model based on current market conditions"""
        try:
            # Extract indicators for model selection
            rsi = self._extract_latest_value(indicators, 'rsi')
            adx = self._extract_latest_value(indicators, 'adx')

            # Simple model selection logic
            if rsi is not None and (rsi < 30 or rsi > 70):
                return 'trend_reversal'
            elif adx is not None and adx > 25:
                return 'momentum'
            else:
                return 'breakout'
        except Exception as e:
            logger.error(f"Error selecting best model: {e}")
            return 'momentum'  # Default to momentum model

    def _extract_latest_price(self, indicators):
        """Extract the latest price from indicators"""
        if 'ema_50' in indicators:
            if isinstance(indicators['ema_50'], (pd.Series, np.ndarray)):
                return indicators['ema_50'].iloc[-1]
            return indicators['ema_50']

        if 'ema_short' in indicators:
            return indicators['ema_short']

        return 100  # Default placeholder value

    def _extract_latest_value(self, indicators, indicator_name):
        """Extract the latest value of a specific indicator"""
        if indicator_name in indicators:
            if isinstance(indicators[indicator_name], (pd.Series, np.ndarray)):
                return indicators[indicator_name].iloc[-1]
            elif isinstance(indicators[indicator_name], dict):
                for key, value in indicators[indicator_name].items():
                    if isinstance(value, (pd.Series, np.ndarray)):
                        return value.iloc[-1]
                    else:
                        return value
            else:
                return indicators[indicator_name]
        return None

    def _predict_trend_reversal(self, symbol, timeframe, indicators, latest_price):
        """Make a trend reversal prediction"""
        try:
            rsi = self._extract_latest_value(indicators, 'rsi')

            if rsi is None:
                return None

            # Simple trend reversal logic
            if rsi < 30:
                direction = 'LONG'
                confidence = 0.65 + ((30 - rsi) / 100)
            elif rsi > 70:
                direction = 'SHORT'
                confidence = 0.65 + ((rsi - 70) / 100)
            else:
                return None

            # Calculate take profit and stop loss
            if direction == 'LONG':
                take_profit = latest_price * 1.03
                stop_loss = latest_price * 0.98
            else:
                take_profit = latest_price * 0.97
                stop_loss = latest_price * 1.02

            return {
                'direction': direction,
                'confidence': min(confidence, 0.95),
                'entry_price': latest_price,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'model_name': 'trend_reversal'
            }
        except Exception as e:
            logger.error(f"Error in trend reversal prediction: {e}")
            return None

    def _predict_momentum(self, symbol, timeframe, indicators, latest_price):
        """Make a momentum prediction"""
        try:
            macd = indicators.get('macd', {})
            macd_line = self._extract_latest_value(macd, 'macd_line')
            signal_line = self._extract_latest_value(macd, 'signal_line')

            if macd_line is None:
                macd_line = indicators.get('macd')

            if signal_line is None:
                signal_line = indicators.get('macd_signal')

            if macd_line is None or signal_line is None:
                return None

            # Momentum logic based on MACD
            if macd_line > signal_line:
                direction = 'LONG'
                confidence = 0.68 + (abs(macd_line - signal_line) / 10)
            elif macd_line < signal_line:
                direction = 'SHORT'
                confidence = 0.68 + (abs(macd_line - signal_line) / 10)
            else:
                return None

            # Calculate take profit and stop loss
            if direction == 'LONG':
                take_profit = latest_price * 1.04
                stop_loss = latest_price * 0.97
            else:
                take_profit = latest_price * 0.96
                stop_loss = latest_price * 1.03

            return {
                'direction': direction,
                'confidence': min(confidence, 0.95),
                'entry_price': latest_price,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'model_name': 'momentum'
            }
        except Exception as e:
            logger.error(f"Error in momentum prediction: {e}")
            return None

    def _predict_breakout(self, symbol, timeframe, indicators, latest_price):
        """Make a breakout prediction"""
        try:
            bb = indicators.get('bollinger_bands', {})
            upper_band = self._extract_latest_value(bb, 'upper')
            lower_band = self._extract_latest_value(bb, 'lower')

            if upper_band is None or lower_band is None:
                return None

            # Breakout logic based on Bollinger Bands
            band_width = (upper_band - lower_band) / latest_price

            if band_width < 0.03:  # Tight bands indicating potential breakout
                # Use ADX to determine direction
                adx = self._extract_latest_value(indicators, 'adx')

                if adx and adx > 20:
                    # Check recent price action
                    ema_50 = self._extract_latest_value(indicators, 'ema_50')
                    if ema_50 is None:
                        ema_50 = self._extract_latest_value(indicators, 'ema_medium')

                    if latest_price > ema_50:
                        direction = 'LONG'
                    else:
                        direction = 'SHORT'

                    confidence = 0.72 + (adx / 100)

                    # Calculate take profit and stop loss
                    if direction == 'LONG':
                        take_profit = latest_price * 1.05
                        stop_loss = latest_price * 0.96
                    else:
                        take_profit = latest_price * 0.95
                        stop_loss = latest_price * 1.04

                    return {
                        'direction': direction,
                        'confidence': min(confidence, 0.95),
                        'entry_price': latest_price,
                        'take_profit': take_profit,
                        'stop_loss': stop_loss,
                        'model_name': 'breakout'
                    }

            return None
        except Exception as e:
            logger.error(f"Error in breakout prediction: {e}")
            return None

    def _store_prediction(self, model_name, symbol, timeframe, prediction):
        """Store a prediction in the database"""
        if not self.db_manager or not prediction:
            return

        try:
            prediction_json = json.dumps({
                'direction': prediction['direction'],
                'entry_price': prediction['entry_price'],
                'take_profit': prediction['take_profit'],
                'stop_loss': prediction['stop_loss']
            })

            self.db_manager.execute_query(
                'ai_model',
                '''
                INSERT INTO predictions
                (model_name, symbol, timeframe, prediction, confidence, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                params=(
                    model_name, symbol, timeframe, prediction_json,
                    prediction['confidence'], int(time.time())
                )
            )
        except Exception as e:
            logger.error(f"Error storing prediction: {e}")

    def train_model(self, model_name):
        """Train or retrain a specific model"""
        if model_name not in self.models:
            logger.error(f"Model {model_name} not found")
            return False

        try:
            logger.info(f"Training model {model_name}")

            # In a real implementation, this would involve actual model training
            # For this example, we'll just update the metadata

            self.models[model_name]['last_trained'] = int(time.time())
            self.models[model_name]['accuracy'] = min(self.models[model_name]['accuracy'] + 0.01, 0.95)

            # Update database
            self._update_model_metadata(model_name)

            return True
        except Exception as e:
            logger.error(f"Error training model {model_name}: {e}")
            return False

    def _update_model_metadata(self, model_name):
        """Update model metadata in the database"""
        if not self.db_manager or model_name not in self.models:
            return

        try:
            model_data = self.models[model_name]
            parameters = json.dumps(model_data.get('parameters', {}))

            self.db_manager.execute_query(
                'ai_model',
                '''
                INSERT OR REPLACE INTO model_metadata
                (model_name, last_trained, accuracy, parameters)
                VALUES (?, ?, ?, ?)
                ''',
                params=(
                    model_name, model_data['last_trained'],
                    model_data['accuracy'], parameters
                )
            )
        except Exception as e:
            logger.error(f"Error updating model metadata: {e}")

    def update_model_accuracy(self, model_name, prediction_id, actual_result, accuracy):
        """Update a model's accuracy based on prediction results"""
        if not self.db_manager or model_name not in self.models:
            return False

        try:
            # Update prediction record
            self.db_manager.execute_query(
                'ai_model',
                '''
                UPDATE predictions
                SET actual_result = ?, accuracy = ?
                WHERE id = ?
                ''',
                params=(actual_result, accuracy, prediction_id)
            )

            # Update model accuracy (weighted average)
            current_accuracy = self.models[model_name]['accuracy']
            new_accuracy = current_accuracy * 0.95 + accuracy * 0.05

            self.models[model_name]['accuracy'] = new_accuracy

            # Update database
            self._update_model_metadata(model_name)

            return True
        except Exception as e:
            logger.error(f"Error updating model accuracy: {e}")
            return False
