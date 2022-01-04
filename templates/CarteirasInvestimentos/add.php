<?php

/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CarteirasInvestimento $carteirasInvestimento
 */
?>
<div class="row">
    <div class="column-responsive column-100">
        <div class="carteirasInvestimentos form content">
            <?= $this->Form->create($carteirasInvestimento) ?>
            <fieldset>
                <legend><?= __('Add Carteiras Investimento') ?></legend>
                <?php
                echo $this->Form->control('usuario_id', ['options' => $usuarios]);
                echo $this->Form->control('nome');
                echo $this->Form->control('descricao');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>