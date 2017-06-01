<?php

class Genghis_GridFsResponse extends Genghis_Response
{
    public function renderHeaders()
    {

        $this->headers['Content-type']        = 'application/octet-stream';
        $this->headers['Content-Disposition'] = 'attachment';

        if (1 == 2) {
            $this->headers['Content-Disposition'] .= sprintf('; filename="%s"', $filename);
        }

        parent::renderHeaders();
    }

    public function renderContent()
    {
        // todo: a better way of handling stream to download
        $stream = stream_get_contents($this->data);

        $exp = explode(',', $stream);
        $base64 = array_pop($exp);
        $stream = base64_decode($base64);

        echo $stream;
    }
}
