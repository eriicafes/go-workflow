import { ogSize, renderOGImage } from '@/lib/og';

export const alt = 'go-workflow: Make Any Go Function Durable';
export const size = ogSize;
export const contentType = 'image/png';

export default function Image() {
  return renderOGImage({ title: 'Make Any Go Function Durable' });
}
