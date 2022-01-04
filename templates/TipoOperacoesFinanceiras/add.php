<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoOperacoesFinanceira $tipoOperacoesFinanceira
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Tipo Operacoes Financeiras'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoOperacoesFinanceiras form content">
            <?= $this->Form->create($tipoOperacoesFinanceira) ?>
            <fieldset>
                <legend><?= __('Add Tipo Operacoes Financeira') ?></legend>
                <?php
                    echo $this->Form->control('nome');
                    echo $this->Form->control('is_aplicacao');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
