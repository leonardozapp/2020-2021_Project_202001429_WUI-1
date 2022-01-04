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
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $tipoOperacoesFinanceira->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $tipoOperacoesFinanceira->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Tipo Operacoes Financeiras'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoOperacoesFinanceiras form content">
            <?= $this->Form->create($tipoOperacoesFinanceira) ?>
            <fieldset>
                <legend><?= __('Edit Tipo Operacoes Financeira') ?></legend>
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
