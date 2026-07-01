import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Viper Chat — live agent POC',
  description: 'Chat UI wired end-to-end through RabbitMQ → worker → Mongo → Redis → SSE',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
