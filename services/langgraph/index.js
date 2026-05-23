const express = require('express');
const app = express();
const port = process.env.PORT || 5000;

app.get('/health', (req, res) => {
  res.json({ status: 'langgraph placeholder', mode: process.env.LANGGRAPH_MODE || 'local' });
});

app.listen(port, () => {
  console.log(`LangGraph stub listening on port ${port}`);
});
