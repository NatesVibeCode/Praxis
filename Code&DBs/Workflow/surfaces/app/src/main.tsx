import React from 'react';
import ReactDOM from 'react-dom/client';
import { App } from './App';
import { installPraxisApiFetch } from './praxis/api';
import './modules';
import './styles/tokens.css';

installPraxisApiFetch();

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
