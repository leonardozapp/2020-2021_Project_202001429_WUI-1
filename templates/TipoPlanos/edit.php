<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoPlano $tipoPlano
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $tipoPlano->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $tipoPlano->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Tipo Planos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoPlanos form content">
            <?= $this->Form->create($tipoPlano) ?>
            <fieldset>
                <legend><?= __('Edit Tipo Plano') ?></legend>
                <?php
                    echo $this->Form->control('nome');
                    echo $this->Form->control('descricao');
                    echo $this->Form->control('permissao_id', ['options' => $permissaos]);
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
