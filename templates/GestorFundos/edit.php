<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\GestorFundo $gestorFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $gestorFundo->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $gestorFundo->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Gestor Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="gestorFundos form content">
            <?= $this->Form->create($gestorFundo) ?>
            <fieldset>
                <legend><?= __('Edit Gestor Fundo') ?></legend>
                <?php
                    echo $this->Form->control('cnpj');
                    echo $this->Form->control('nome');
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
