from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd

from langchain_openai import ChatOpenAI
from langchain.messages import HumanMessage
from langchain.messages import SystemMessage

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.exchange.local import LocalExchangeFactory
from academy.handle import Handle
from academy.logging import init_logging
from academy.manager import Manager

EXCHANGE_ADDRESS = 'https://exchange.academy-agents.org'
logger = logging.getLogger(__name__)

class Monitor(Agent):

    POLL_INTERVAL = 60
    POLL_URL = "https://datarepr.duckdns.org/api/v1/sentinel/signals/?format=json"

    def __init__(
        self,
        review: Handle[Review],
        model: str,
        access_token: str,
     ) -> None:
        super().__init__()
        self.model = model
        self.access_token = access_token
        self.review = review
        self.llm: ChatOpenAI | None = None 

    async def agent_on_startup(self) -> None:
        self.llm = ChatOpenAI(model=self.model,
            api_key=self.access_token,
        )
        self.latest_signals_df: pd.DataFrame = pd.DataFrame()
        self.all_signals_df: pd.DataFrame = pd.DataFrame()

    async def agent_on_shutdown(self) -> None:
        self.agent_shutdown()

    @loop
    async def poll_signals(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            print('Polling for signals...')
            try:
                signals_df = await asyncio.to_thread(self._fetch_signals, self.POLL_URL)
                new_signals_available = await self._process_signals(signals_df) 
                if new_signals_available:
                    latest_signal_alert = await self.classify_latest_signal_alert()
                    await self.review.review_latest_signal_alert(latest_signal_alert.get('id'), latest_signal_alert.get('classification'))
                else: 
                    print('No new signals to classify at this time.')
            except Exception:
                logger.exception('Monitor polling failed')
            print(f'Waiting for {self.POLL_INTERVAL} seconds before next poll...')
            await asyncio.sleep(self.POLL_INTERVAL)
    

    def _fetch_signals(self, POLL_URL: str) -> pd.DataFrame:
        # Fetch data
        response = requests.get(POLL_URL, timeout=30)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data)
        if "id" in df.columns:
            df.set_index("id", inplace=True)

        return df
    
    async def _process_signals(self, df: pd.DataFrame) -> bool:

        new_vals = df.index.values
        old_vals = self.all_signals_df.index.values

        i, j = len(old_vals), len(new_vals)
        n = j-i

        if n <= 0:
            print('No new signals found')
            return False
        else:
            print(f'Found {n} new signals', df.iloc[:n])

            self.all_signals_df = pd.concat([df.iloc[:n],self.all_signals_df], ignore_index=False)
            print('Updated all signals dataframe shape:', self.all_signals_df.index)
            self.latest_signals_df = df.iloc[:n] 
            print('Updated latest signals dataframe shape:', self.latest_signals_df.index)
            return True
    
    def _query_llm_sync(self, system_prompt: str, user_prompt: str) -> str:
        if self.llm is None:
            raise RuntimeError('LLM not initialised — agent_on_startup may not have run.')
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response = self.llm.invoke(messages)
        return response.content
    
    @action
    async def classify_latest_signal_alert(self) -> dict[str, str]:

        if self.latest_signals_df.empty:
            return {
                'classification': 'no_signal',
                'reason': 'No latest signal is available to classify.',
            }
        print('Latest signals dataframe shape in classify_latest_signal_alert:', self.latest_signals_df.head(5))
        records = self.latest_signals_df.reset_index().to_dict(orient='records')
        signal_data = random.choice(records)
        print('Classifying signal with id:', signal_data.get('id'))
        signal_json = json.dumps(signal_data, default=str, ensure_ascii=False)

        system_prompt = (
            'You are an expert epidemiologist specialising in early disease-outbreak '
            'detection. Your job is to assess individual signals from a disease-surveillance '
            'platform and decide whether they warrant alerting public health officials.\n\n'
            'Respond in valid JSON with exactly two keys:\n'
            '  "classification": one of "area alert", "continent alert", '
            '"no alert needed", or "uncertain"\n'
            '  "rationale": a concise explanation (2-3 sentences) grounded in the '
            'signal data.\n\n'
            'Do not include any text outside the JSON object.'
        )

        user_prompt = (
            'Below is a single disease-surveillance signal record. '
            'Classify whether it should trigger a public-health alert at the '
            'area level, continent level, or no alert at all.\n\n'
            f'Signal record:\n{signal_json}'
        )

        raw_response = await asyncio.to_thread(self._query_llm_sync, system_prompt, user_prompt)

        try:
            print('Raw LLM response:', raw_response)
            parsed = json.loads(raw_response)
            classification = parsed.get('classification', 'uncertain')
            rationale = parsed.get('rationale', raw_response)
        except json.JSONDecodeError:
            logger.warning('LLM returned non-JSON response; storing raw text as rationale.')
            classification = 'uncertain'
            rationale = raw_response

        logger.info(
            'Signal classification: %s | signal id: %s',
            classification,
            signal_data.get('id'),
        )

        return {
            'id': signal_data.get('id'),
            'classification': classification,
            'rationale': rationale,
            'signal': signal_json,
        }


class Review(Agent):

    alert_history = 10

    def __init__(
        self,
        notification: Handle[Notification],
        model: str,
        access_token: str,    
     ) -> None:
        super().__init__()
        self.model = model
        self.access_token = access_token
        self.notification = notification
        self.llm: ChatOpenAI | None = None 

    async def agent_on_startup(self) -> None:
        self.llm = ChatOpenAI(model=self.model,
            api_key=self.access_token,
        )
        self.latest_alerts: pd.DataFrame = pd.DataFrame()

    async def agent_on_shutdown(self) -> None:
        self.agent_shutdown()

    def _query_llm_sync(self, system_prompt: str, user_prompt: str) -> str:
        if self.llm is None:
            raise RuntimeError('LLM not initialised — agent_on_startup may not have run.')
        response = self.llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ])
        return response.content
    
    @action
    async def review_latest_signal_alert(self, id: str, classification: str) -> int:
        print("Received alert for review:", id)

        alert_df = pd.DataFrame([{'id': id, 'classification': classification}])
        alert_df = alert_df.set_index('id')
        self.latest_alerts = pd.concat([alert_df, self.latest_alerts])
        if self.latest_alerts.size > self.alert_history:
            self.latest_alerts = self.latest_alerts.iloc[:self.alert_history]

        if len(self.latest_alerts) > 1:
            history_records = self.latest_alerts.iloc[1:].reset_index().to_dict(orient='records')
            history_str = json.dumps(history_records, default=str, ensure_ascii=False, indent=2)
        else:
            history_str = 'No previous alerts on record — this is the first alert.'
        print('History of recent alerts for review:', history_str)
 
        system_prompt = (
            'You are a senior public health intelligence analyst. '
            'You are given a newly classified disease-surveillance alert and a history '
            'of recent past alerts. Your job is to:\n'
            '1. Assess the SEVERITY of the new alert relative to the historical baseline '
            '(low / moderate / high / critical).\n'
            '2. Determine the NOTIFICATION SCOPE: who needs to be informed?\n'
            '   - "local": local health authorities and clinicians in the affected area only\n'
            '   - "continental": regional/continental health bodies (e.g. Africa CDC, ECDC)\n'
            '   - "worldwide": global bodies (e.g. WHO) plus all of the above\n'
            '3. Describe the specific GROUPS TO NOTIFY in plain English.\n\n'
            'Respond in valid JSON with exactly four keys:\n'
            '  "severity": one of "low", "moderate", "high", "critical"\n'
            '  "notification_scope": one of "local", "continental", "worldwide"\n'
            '  "notify_groups": a concise plain-English list of who to notify\n'
            '  "reasoning": 1-2 sentences explaining your assessment, explicitly '
            'referencing how this alert compares to the history.\n\n'
            'Do not include any text outside the JSON object.'
        )
 
        user_prompt = (
            'NEW ALERT\n'
            f'Signal ID: {id}\n'
            f'Classification: {classification}\n'
            'RECENT ALERT HISTORY (most recent first):\n'
            f'{history_str}'
        )
 
        raw_response = await asyncio.to_thread(self._query_llm_sync, system_prompt, user_prompt)
 
        try:
            parsed = json.loads(raw_response)
            severity = parsed.get('severity', 'uncertain')
            scope = parsed.get('notification_scope', 'local')
            notify_groups = parsed.get('notify_groups', '')
            reasoning = parsed.get('reasoning', raw_response)
        except json.JSONDecodeError:
            logger.warning('Review: LLM returned non-JSON; storing raw as reasoning.')
            severity = 'uncertain'
            scope = 'local'
            notify_groups = 'Unable to parse — defaulting to local authorities.'
            reasoning = raw_response
    
        logger.info(
            'Review: severity=%s scope=%s signal_id=%s',
            severity, scope, id,
        )

        await self.notification.notify({
            'severity': severity,
            'notification_scope': scope,
            'notify_groups': notify_groups,
            'reasoning': reasoning,
        })

        return 0

    
class Notification(Agent):

    async def agent_on_shutdown(self) -> None:
        self.agent_shutdown()

        
    @action
    async def notify(self, text: dict) -> None:
        print('NOTIFICATION:', text)
        print('Sending alert to:', text.get('notify_groups'))
        return None
        
 
async def main() -> int:
    init_logging(logging.INFO)

    OPENAI_API_KEY="Enter your key here"

    local_executor = ThreadPoolExecutor()

    shutdown = asyncio.Event()
    event_loop = asyncio.get_running_loop()
    event_loop.add_signal_handler(signal.SIGINT, shutdown.set)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown.set)

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=local_executor,
    ) as manager:
        # Launch each of the three agents types. The returned type is
        # a handle to that agent used to invoke actions.

        notification = await manager.launch(Notification)
        print('Notification agent launched')

        review = await manager.launch(
            Review,
            args=(notification,),
            kwargs={
                "model": "gpt-4o-mini",
                "access_token": OPENAI_API_KEY,
            }
        )
        print('Review agent launched')

        monitor = await manager.launch(
            Monitor,
            args=(review,),
            kwargs={
                "model": "gpt-4o-mini",
                "access_token": OPENAI_API_KEY,
            }
        ) 
        print('Monitor agent launched')      

        logger.info('All agents launched — running until Ctrl+C')
        await shutdown.wait()
        logger.info('Shutdown signal received — exiting cleanly.')

        await manager.shutdown(monitor, blocking=True)
        await manager.shutdown(review, blocking=True)
        await manager.shutdown(notification, blocking=True)

    local_executor.shutdown(wait=True)
    
    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
