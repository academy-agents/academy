# CEPi Agent System

## Overview

This Python script implements a multi-agent system for monitoring disease surveillance signals using the Academy agents framework. The system polls an external API for new disease signals, classifies them using a Large Language Model (LLM), reviews alerts in historical context, and sends notifications based on severity and scope.

## Features

- **Signal Monitoring**: Continuously polls a disease surveillance API for new signals.
- **LLM-Based Classification**: Uses OpenAI's GPT-4o-mini to classify signals as alerts (area, continent, or no alert).
- **Alert Review**: Analyzes new alerts against recent history to determine severity and notification scope.
- **Notification System**: Sends notifications to appropriate health authorities based on the review.
- **Asynchronous Operation**: Built with asyncio for non-blocking execution.

## Agents

### Monitor Agent
- Polls the API every 60 seconds for new signals.
- Processes and stores signals in pandas DataFrames.
- Classifies the latest signal using LLM and sends it to the Review agent.

### Review Agent
- Receives classified alerts from Monitor.
- Maintains a history of recent alerts (up to 10).
- Uses LLM to assess severity (low/moderate/high/critical) and notification scope (local/continental/worldwide).
- Forwards notifications to the Notification agent.

### Notification Agent
- Receives notification details and prints them (placeholder for actual notification system).

## Configuration

Before running the script, you need to set your OpenAI API key:

1. Open the script file `cepi-agents.py`.
2. Replace `"Enter your key here"` in the `OPENAI_API_KEY` variable with your actual OpenAI API key.

## Usage

Run the script from the command line:

```
python cepi-agents.py
```

The system will start polling the API and processing signals. It will run indefinitely until interrupted with Ctrl+C.

### Output

- Console logs showing polling status, new signals found, classifications, and notifications.
- The system prints notifications to the console, including severity, scope, groups to notify, and reasoning.

## API Details

- **Poll URL**: `https://datarepr.duckdns.org/api/v1/sentinel/signals/?format=json`
- **Poll Interval**: 60 seconds
- Signals are expected in JSON format with an `id` field.