<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
?>
<h2>
    <?= __("Responda o questionário abaixo para conhecermos seu perfil de investidor. Como seu perfil pode mudar com o tempo, sinta-se a vontade para sempre que desejar responder novamente.") ?>
</h2>

<?= $this->Form->create($usuario) ?>
<fieldset>
    <?php

    echo $this->Form->label('favorite_color', 'Sobre tolerância a riscos');
    echo '<div class="inline_labels">';
    echo $this->Form->radio(
        'question_1',
        [
            ['value' => 'A', 'text' => ' A. Irei perder noites de sono caso o meu rendimento caia.'],
            ['value' => 'B', 'text' => ' B. Consigo lidar com algumas perdas, desde que moderadas.'],
            ['value' => 'C', 'text' => ' C. Consigo ficar tranquilo com perdas, mesmo que do capital principal, confiante que minha decisão fará com que eu ganhe no longo prazo.', 'style' => 'color:green;'],
        ],
        ['required' => true],
    );
    echo '</div>';

    echo $this->Form->label('favorite_color', 'Sobre a situação financeira');
    echo '<div class="inline_labels">';
    echo $this->Form->radio(
        'question_2',
        [
            ['value' => 'A', 'text' => ' A. Vou investir um dinheiro que posso precisar se tiver uma emergência.'],
            ['value' => 'B', 'text' => ' B. Vou investir um dinheiro, mas tenho uma reserva que posso recorrer em emergências.'],
            ['value' => 'C', 'text' => ' C. Vou investir, mas tenho seguros e outros recursos que me darão segurança em qualquer eventualidade.', 'style' => 'color:green;'],
        ],
        ['required' => true],
    );
    echo '</div>';

    echo $this->Form->label('favorite_color', 'Sobre objetivo');
    echo '<div class="inline_labels">';
    echo $this->Form->radio(
        'question_3',
        [
            ['value' => 'A', 'text' => ' A. Se meu objetivo atrasar ou for alterado, isso terá um efeito grave.'],
            ['value' => 'B', 'text' => ' B. Possuo flexibilidade no valor que preciso para meu objetivo ou posso esperar mais, sem grandes consequências.'],
            ['value' => 'C', 'text' => ' C. Tenho bastante tempo para atingir meu objetivo.', 'style' => 'color:green;'],
        ],
        ['required' => true],
    );
    echo '</div>';

    echo $this->Form->label('favorite_color', 'Sobre conhecimento sobre o mercado');
    echo '<div class="inline_labels">';
    echo $this->Form->radio(
        'question_4',
        [
            ['value' => 'A', 'text' => ' A. Estou começando minha educação financeira, não conheço índices e não sei onde me informar.'],
            ['value' => 'B', 'text' => ' B. Entendo alguns índices, acesso algumas fontes oficiais de informação e me dedico para aprender mais.'],
            ['value' => 'C', 'text' => ' C. Entendo sobre vários índices, conheço diferenes fontes de informação, sei analisar o mercado.', 'style' => 'color:green;'],
        ],
        ['required' => true],
    );
    echo '</div>';
    ?>
</fieldset>
<?= $this->Form->button(__('Enviar')) ?>
<?= $this->Form->end() ?>